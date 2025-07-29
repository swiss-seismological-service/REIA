import configparser
import io
import pickle
from pathlib import Path

from reia.io.calculation import validate_and_parse_calculation_input
from reia.repositories.calculation import (CalculationBranchRepository,
                                           CalculationRepository)
from reia.repositories.types import SessionType
from reia.schemas.calculation_schemas import CalculationBranchSettings
from reia.schemas.enums import EStatus
from reia.services.exposure import create_exposure_inputs
from reia.services.fragility import create_fragility_input
from reia.services.logger import LoggerService
from reia.services.oq_api import OQCalculationAPI
from reia.services.results import ResultsService
from reia.services.status_tracker import StatusTracker
from reia.services.taxonomy import create_taxonomymap_input
from reia.services.vulnerability import create_vulnerability_input
from reia.utils import create_file_buffer_configparser
from settings import get_config


class CalculationService:
    """Service for managing OpenQuake calculations."""

    def __init__(self, session: SessionType):
        # Initialize logging for calculation workflows
        LoggerService.setup_logging()
        self.logger = LoggerService.get_logger(__name__)

        self.session = session
        self.config = get_config()
        self.status_tracker = StatusTracker(session)

    def run_calculations(
            self,
            branch_settings: list[CalculationBranchSettings]):
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
                         f"{len(branch_settings)} branches")
        calculation, branches_dicts = validate_and_parse_calculation_input(
            branch_settings)

        # Create calculation and branches in database
        calculation = CalculationRepository.create(self.session, calculation)
        self.logger.info(f"Created calculation {calculation.oid}")
        for b in branches_dicts:
            b.calculation_oid = calculation.oid
        branches = [CalculationBranchRepository.create(self.session, b)
                    for b in branches_dicts]

        try:
            # Update calculation status to executing
            calculation = self.status_tracker.update_status(
                calculation,
                EStatus.EXECUTING,
                "Starting calculation processing")

            # Process each calculation branch
            for i, (setting, branch) in enumerate(
                    zip(branch_settings, branches), 1):
                self.logger.info(
                    "Processing calculation branch "
                    f"{i}/{len(branches)} (ID: {branch.oid})")
                branch = self._run_single_calculation(setting, branch)

            # Determine final status
            branches = [CalculationBranchRepository.get_by_id(
                self.session, b.oid) for b in branches]

            status = self.status_tracker.validate_calculation_completion(
                branches)
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
                                setting: CalculationBranchSettings,
                                branch):
        """Run a single calculation branch using OQCalculationAPI.

        Args:
            setting: Configuration for the calculation
            branch: Database branch object

        Returns:
            Updated branch object
        """
        # Create API client
        api_client = OQCalculationAPI(self.config)

        # Prepare calculation files
        self.logger.debug(
            f"Preparing calculation files for branch {branch.oid}")
        files = assemble_calculation_input(self.session, setting.config)
        api_client.add_calc_files(*files)

        # Run calculation and wait for completion
        self.logger.info(
            f"Submitting calculation branch {branch.oid} to OpenQuake engine")
        final_status = api_client.run()
        self.logger.info(f"OpenQuake calculation for branch {branch.oid} "
                         f"finished with status: {final_status}")

        # Update branch status
        status = EStatus[final_status.upper()]

        # Log OpenQuake traceback if calculation failed
        if status == EStatus.FAILED:
            api_client.log_error_with_traceback(
                f"OpenQuake calculation failed for branch {branch.oid}")

        branch = self.status_tracker.update_status(
            branch,
            status,
            f"OpenQuake calculation completed with status: {final_status}")

        # Save results if calculation completed successfully
        if branch.status == EStatus.COMPLETE:
            self.logger.info(
                f'Saving results for calculation branch {branch.oid} '
                f'with weight {branch.weight}')
            results_service = ResultsService(self.session, api_client)
            results_service.save_calculation_results(branch)

        return branch


def create_calculation_files_to_folder(session: SessionType,
                                       settings_file: Path,
                                       target_folder: Path) -> bool:
    """Export calculation configuration from data storage layer to disk files.

    Args:
        session: Database session.
        settings_file: Path to the settings file.
        target_folder: Folder where to create the files.

    Returns:
        True if files were created successfully.
    """
    target_folder.mkdir(exist_ok=True)

    job_file = configparser.ConfigParser()
    job_file.read(settings_file)

    files = assemble_calculation_input(session, job_file)

    for file in files:
        with open(target_folder / file.name, 'w') as f:
            f.write(file.getvalue())

    return True


def run_test_calculation(session: SessionType, settings_file: Path) -> str:
    """Generate calculation from data storage layer and submit to OpenQuake.

    Args:
        session: Database session.
        settings_file: Path to the settings file.

    Returns:
        Response from OpenQuake API.
    """
    job_file = configparser.ConfigParser()
    job_file.read(settings_file)

    config = get_config()
    api_client = OQCalculationAPI(config)

    files = assemble_calculation_input(session, job_file)
    api_client.add_calc_files(*files)

    response = api_client.submit()
    return response


def run_calculation_from_files(session: SessionType,
                               settings_files: list[str],
                               weights: list[float]) -> None:
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

    # Parse settings files into CalculationBranchSettings
    branch_settings = []
    for weight, settings_file in zip(weights, settings_files):
        job_file = configparser.ConfigParser()
        job_file.read(Path(settings_file))
        branch_settings.append(
            CalculationBranchSettings(weight=weight, config=job_file))

    # Run calculations using the service
    calc_service = CalculationService(session)
    calc_service.run_calculations(branch_settings)


def assemble_calculation_input(session: SessionType,
                               job: configparser.ConfigParser
                               ) -> list[io.StringIO]:
    """Generate calculation input files from data storage
    layer to in-memory files.

    Creates in-memory file objects for all calculation inputs including
    exposure, vulnerability/fragility, taxonomy mapping, hazard files,
    and the job configuration file.

    Args:
        session: Database session.
        job: ConfigParser object containing calculation configuration.

    Returns:
        List of in-memory file objects for the calculation.
    """
    # create deep copy of configparser
    tmp = pickle.dumps(job)
    working_job = pickle.loads(tmp)

    calculation_files = []

    exposure_xml, exposure_csv = create_exposure_inputs(
        session, working_job['exposure']['exposure_file'])
    exposure_xml.name = 'exposure.xml'
    working_job['exposure']['exposure_file'] = exposure_xml.name

    calculation_files.extend([exposure_xml, exposure_csv])

    if 'vulnerability' in working_job.keys():
        for k, v in working_job['vulnerability'].items():
            if k == 'taxonomy_mapping_csv':
                file = create_taxonomymap_input(session, v)
                file.name = "{}.csv".format(k.replace('_file', ''))
            else:
                file = create_vulnerability_input(session, v)
                file.name = "{}.xml".format(k.replace('_file', ''))
            working_job['vulnerability'][k] = file.name
            calculation_files.append(file)

    elif 'fragility' in working_job.keys():
        for k, v in working_job['fragility'].items():
            if k == 'taxonomy_mapping_csv':
                file = create_taxonomymap_input(session, v)
            else:
                file = create_fragility_input(session, v)
                file.name = "{}.xml".format(k.replace('_file', ''))
            working_job['fragility'][k] = file.name
            calculation_files.append(file)

    for k, v in working_job['hazard'].items():
        with open(v, 'r') as f:
            file = io.StringIO(f.read())
        file.name = Path(v).name
        working_job['hazard'][k] = file.name
        calculation_files.append(file)

    job_file = create_file_buffer_configparser(working_job, 'job.ini')

    calculation_files.append(job_file)

    return calculation_files
