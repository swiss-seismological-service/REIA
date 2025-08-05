import configparser
import io
import pickle
from pathlib import Path

from reia.io.calculation import (create_calculation, create_calculation_branch,
                                 validate_calculation_input)
from reia.repositories.calculation import (CalculationBranchRepository,
                                           CalculationRepository)
from reia.repositories.types import SessionType
from reia.schemas.calculation_schemas import (Calculation,
                                              CalculationBranchSettings)
from reia.schemas.enums import EStatus
from reia.services import DataService
from reia.services.exposure import ExposureService
from reia.services.fragility import FragilityService
from reia.services.logger import LoggerService
from reia.services.oq_api import OQCalculationAPI
from reia.services.results import ResultsService
from reia.services.status_tracker import StatusTracker
from reia.services.taxonomy import TaxonomyService
from reia.services.vulnerability import VulnerabilityService
from reia.utils import create_file_buffer_configparser
from reia.config.settings import get_settings


class CalculationService:
    """Service for managing OpenQuake calculations."""

    def __init__(self, session: SessionType):
        # Initialize logging for calculation workflows
        LoggerService.setup_logging()
        self.logger = LoggerService.get_logger(__name__)

        self.session = session
        self.config = get_settings()
        self.status_tracker = StatusTracker(session)

    def run_calculations(
            self,
            calculation: Calculation,
            branch_settings: list[CalculationBranchSettings]
    ) -> Calculation:
        """Run OpenQuake calculations using the existing workflow.

        Args:
            branch_settings: List of calculation branch settings

        Returns:
            Completed calculation object

        Raises:
            Exception: If validation or calculation fails
        """
        # Validate and parse inputs using io layer
        self.logger.info("Starting calculation workflow with "
                         f"{len(branch_settings)} branches.")

        self.logger.info(
            f"Running calculation {calculation.oid} with "
            f"branches {[b.branch.oid for b in branch_settings]}.")

        try:
            # Update calculation status to executing
            calculation = self.status_tracker.update_status(
                calculation,
                EStatus.EXECUTING,
                "Starting calculation processing")

            # Process each calculation branch
            for i, b in enumerate(branch_settings):
                self.logger.info(
                    "Executing calculation branch "
                    f"{i}/{len(branch_settings)} (ID: {b.branch.oid})")
                b = self._run_single_calculation(b)

            # Determine final status
            status = self.status_tracker.validate_calculation_completion(
                [b.branch for b in branch_settings])

            calculation = self.status_tracker.update_status(
                calculation,
                status,
                "All calculation branches completed")

            calculation = CalculationRepository.get_by_id(
                self.session, calculation.oid)

            self.logger.info(
                f"Calculation {calculation.oid} completed "
                f"with final status: {status.name}")
            return calculation

        except BaseException as e:
            # Handle failures with rollback
            self.session.rollback()
            for el in self.session.identity_map.values():
                if hasattr(el, 'status') and el.status != EStatus.COMPLETE:
                    el.status = EStatus.ABORTED if isinstance(
                        e, KeyboardInterrupt) else EStatus.FAILED
                    self.session.commit()
            raise e

    def _run_single_calculation(self,
                                setting: CalculationBranchSettings
                                ) -> CalculationBranchSettings:
        """Run a single calculation branch using OQCalculationAPI.

        Args:
            setting: Configuration for the calculation

        Returns:
            Updated branch object
        """
        # Create API client
        api_client = OQCalculationAPI(self.config)

        # Prepare calculation files
        self.logger.debug(
            f"Preparing calculation files for branch {setting.branch.oid}")

        files = CalculationDataService.export_branch_to_buffer(
            self.session, setting.config)

        api_client.add_calc_files(*files)

        # Run calculation and wait for completion
        self.logger.info(f"Submitting calculation branch {setting.branch.oid} "
                         "to OpenQuake engine")

        final_status = api_client.run()
        self.logger.info(
            f"OpenQuake calculation for branch {setting.branch.oid} "
            f"finished with status: {final_status}")

        # Update branch status
        status = EStatus[final_status.upper()]

        # Log OpenQuake traceback if calculation failed
        if status == EStatus.FAILED:
            api_client.log_error_with_traceback(
                "OpenQuake calculation failed for "
                f"branch {setting.branch.oid}")

        setting.branch = self.status_tracker.update_status(
            setting.branch,
            status,
            f"OpenQuake calculation completed with status: {final_status}")

        # Save results if calculation completed successfully
        if setting.branch.status == EStatus.COMPLETE:
            self.logger.info(
                f'Saving results for calculation branch {setting.branch.oid} '
                f'with weight {setting.weight}')
            results_service = ResultsService(self.session, api_client)
            results_service.save_calculation_results(setting.branch)

        return setting


def run_test_calculation(session: SessionType, settings_file: Path) -> str:
    """Generate calculation from data storage layer and submit to OpenQuake.

    Args:
        session: Database session.
        settings_file: Path to the settings file.

    Returns:
        Response from OpenQuake API.
    """
    config = get_settings()
    api_client = OQCalculationAPI(config)

    files = CalculationDataService.export_branch_to_buffer(
        session, settings_file)
    api_client.add_calc_files(*files)

    response = api_client.submit()
    return response


def run_calculation_from_files(session: SessionType,
                               settings_files: list[str],
                               weights: list[float]) -> Calculation:
    """Run OpenQuake calculation from multiple settings files.

    Args:
        session: Database session.
        settings_files: List of paths to calculation settings files.
        weights: List of weights for calculation branches.

    Raises:
        ValueError: If number of settings files and weights don't match.
    """
    # Validate input
    if len(settings_files) != len(weights):
        raise ValueError('Number of setting files and weights must be equal.')

    # Validate and load calculation and branches
    calculation, branch_settings = CalculationDataService.import_from_file(
        session, settings_files, weights)

    # Run calculations using the service
    calc_service = CalculationService(session)
    calculation = calc_service.run_calculations(calculation, branch_settings)

    return calculation


class CalculationDataService(DataService):
    @classmethod
    def import_from_file(cls,
                         session: SessionType,
                         config_path: list[Path],
                         weights: list[int]
                         ) -> tuple[Calculation,
                                    list[CalculationBranchSettings]]:
        """Load data from a file and store it via the repository."""
        if len(config_path) != len(weights):
            raise ValueError(
                'Number of setting files and weights must be equal.')

        branch_settings = []

        for path, weight in zip(config_path, weights):
            config = configparser.ConfigParser()
            config.read(path)
            branch = create_calculation_branch(config, weight)
            setting = CalculationBranchSettings(
                weight=weight, config=config, branch=branch)
            branch_settings.append(setting)

        validate_calculation_input(branch_settings)

        calculation = create_calculation(branch_settings)

        calculation = CalculationRepository.create(session, calculation)

        for b in branch_settings:
            b.branch.calculation_oid = calculation.oid
            b.branch = CalculationBranchRepository.create(session, b.branch)

        return calculation, branch_settings

    @classmethod
    def export_branch_to_directory(
            cls,
            session: SessionType,
            config_path: Path,
            output_directory: Path) -> Path:
        """Export calculation config from data storage layer to disk files.

        Args:
            session: Database session.
            config_path: Path to the calculation settings file.
            output_directory: Directory where to create the files.

        Returns:
            Path to the created directory.
        """
        # Create output directory if it doesn't exist
        output_directory.mkdir(exist_ok=True)

        # Generate in-memory files using the buffer method
        files = cls.export_branch_to_buffer(session, config_path)

        # Write all files to disk
        for file in files:
            file_path = output_directory / file.name
            with open(file_path, 'w') as f:
                f.write(file.getvalue())

        return output_directory

    @classmethod
    def export_branch_to_buffer(
            cls,
            session: SessionType,
            config: Path | configparser.ConfigParser) -> list[io.StringIO]:
        """Generate calculation input files from data storage to memory.

        Creates in-memory file objects for all calculation inputs including
        exposure, vulnerability/fragility, taxonomy mapping, hazard files,
        and the job configuration file.

        Args:
            session: Database session.
            config_path: Path to calculation settings file.

        Returns:
            List of in-memory file objects for the calculation.
        """
        # Read and create deep copy of configparser

        if isinstance(config, Path):
            working_job = configparser.ConfigParser()
            working_job.read(str(config))
        elif isinstance(config, configparser.ConfigParser):
            tmp = pickle.dumps(config)
            working_job = pickle.loads(tmp)
        else:
            raise ValueError("config must be a Path or ConfigParser instance.")

        calculation_files = []

        # Generate exposure files
        exposure_xml, exposure_csv = ExposureService.export_to_buffer(
            session, working_job['exposure']['exposure_file'])
        exposure_xml.name = 'exposure.xml'
        working_job['exposure']['exposure_file'] = exposure_xml.name

        calculation_files.extend([exposure_xml, exposure_csv])

        # Generate vulnerability or fragility files
        if 'vulnerability' in working_job.keys():
            for k, v in working_job['vulnerability'].items():
                if k == 'taxonomy_mapping_csv':
                    file = TaxonomyService.export_to_buffer(session, v)
                    file.name = "{}.csv".format(k.replace('_file', ''))
                else:
                    file = VulnerabilityService.export_to_buffer(session, v)
                    file.name = "{}.xml".format(k.replace('_file', ''))
                working_job['vulnerability'][k] = file.name
                calculation_files.append(file)

        elif 'fragility' in working_job.keys():
            for k, v in working_job['fragility'].items():
                if k == 'taxonomy_mapping_csv':
                    file = TaxonomyService.export_to_buffer(session, v)
                    file.name = "{}.csv".format(k.replace('_file', ''))
                else:
                    file = FragilityService.export_to_buffer(session, v)
                    file.name = "{}.xml".format(k.replace('_file', ''))
                working_job['fragility'][k] = file.name
                calculation_files.append(file)

        # Copy hazard files from disk to memory
        for k, v in working_job['hazard'].items():
            with open(v, 'r') as f:
                file = io.StringIO(f.read())
            file.name = Path(v).name
            working_job['hazard'][k] = file.name
            calculation_files.append(file)

        # Generate job configuration file
        job_file = create_file_buffer_configparser(working_job, 'job.ini')
        calculation_files.append(job_file)

        return calculation_files
