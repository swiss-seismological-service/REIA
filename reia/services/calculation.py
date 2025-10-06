import configparser
import io
from copy import deepcopy
from datetime import datetime
from pathlib import Path

from reia.config.settings import get_settings
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
from reia.services.gmfs import GMFConfigurationService
from reia.services.logger import LoggerService
from reia.services.oq_api import OQCalculationAPI
from reia.services.results import ResultsService
from reia.services.status_tracker import StatusTracker
from reia.services.taxonomy import TaxonomyService
from reia.services.vulnerability import VulnerabilityService
from reia.utils import create_file_buffer_configparser


class CalculationExecutionService:
    """Service for managing OpenQuake calculations."""

    def __init__(self, session: SessionType):
        self.logger = LoggerService.get_logger(__name__)

        self.session = session
        self.app_settings = get_settings()
        self.status_tracker = StatusTracker(session)

    def run_calculation(
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
            f"Running calculation with id {calculation.oid} and "
            f"branch ids {[b.branch.oid for b in branch_settings]}.")

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
                    f"{i + 1}/{len(branch_settings)} (ID: {b.branch.oid})")
                b = self._run_calculation_branch(b)

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

    def _run_calculation_branch(self,
                                branch_setting: CalculationBranchSettings
                                ) -> CalculationBranchSettings:
        """Run a single calculation branch using OQCalculationAPI.

        Args:
            setting: Configuration for the calculation

        Returns:
            Updated branch object
        """
        # Create API client
        api_client = OQCalculationAPI(self.app_settings)

        # Prepare calculation files
        self.logger.debug(
            "Preparing calculation files for branch "
            f"{branch_setting.branch.oid}")

        files = CalculationDataService.export_branch_to_buffer(
            self.session, branch_setting.config)

        api_client.add_calc_files(*files)

        # Run calculation and wait for completion
        self.logger.info(
            f"Submitting calculation branch {branch_setting.branch.oid} "
            "to OpenQuake engine")

        final_status = api_client.run()
        self.logger.info(
            f"OpenQuake calculation for branch {branch_setting.branch.oid} "
            f"finished with status: {final_status}")

        # Update branch status
        status = EStatus[final_status.upper()]

        # Log OpenQuake traceback if calculation failed
        if status == EStatus.FAILED:
            api_client.log_error_with_traceback(
                "OpenQuake calculation failed for "
                f"branch {branch_setting.branch.oid}")

        branch_setting.branch = self.status_tracker.update_status(
            branch_setting.branch,
            status,
            f"OpenQuake calculation completed with status: {final_status}")

        # Save results if calculation completed successfully
        if branch_setting.branch.status == EStatus.COMPLETE:
            self.logger.info(
                'Saving results for calculation branch '
                f'{branch_setting.branch.oid} '
                f'with weight {branch_setting.weight}')
            results_service = ResultsService(self.session,
                                             api_client=api_client)
            results_service.save_calculation_results(branch_setting.branch)

        return branch_setting


def run_test_calculation(session: SessionType, settings_file: Path) -> str:
    """Generate calculation from data storage layer and submit to OpenQuake.

    Args:
        session: Database session.
        settings_file: Path to the settings file.

    Returns:
        Response from OpenQuake API.
    """
    api_client = OQCalculationAPI(get_settings())

    files = CalculationDataService.export_branch_to_buffer(
        session, settings_file)
    api_client.add_calc_files(*files)

    response = api_client.submit()
    return response


class CalculationDataService(DataService):
    @classmethod
    def import_from_files(cls,
                          session: SessionType,
                          config_path: list[Path],
                          weights: list[int],
                          origin_time: datetime | None = None) \
        -> tuple[tuple[Calculation,
                       list[CalculationBranchSettings]],
                 tuple[Calculation,
                       list[CalculationBranchSettings]]]:
        """Load data from a file and store it via the repository."""
        if len(config_path) != len(weights):
            raise ValueError(
                'Number of setting files and weights must be equal.')

        losscalculation = None
        lossbranches = []
        damagecalculation = None
        damagebranches = []

        for path, weight in zip(config_path, weights):
            config = configparser.ConfigParser(interpolation=None)
            config.read(path)

            if origin_time is not None:
                config = add_time(config, origin_time)

            if config.has_section('vulnerability'):
                config_loss = deepcopy(config)
                config_loss['general']['calculation_mode'] = 'scenario_risk'
                branch = create_calculation_branch(config_loss, weight)
                setting = CalculationBranchSettings(
                    weight=weight, config=config_loss, branch=branch)
                lossbranches.append(setting)

            if config.has_section('fragility'):
                config_damage = deepcopy(config)
                config_damage['general']['calculation_mode'] = 'scenario_damage'
                branch = create_calculation_branch(config_damage, weight)
                setting = CalculationBranchSettings(
                    weight=weight, config=config_damage, branch=branch)
                damagebranches.append(setting)

        if lossbranches:
            validate_calculation_input(lossbranches)
            losscalculation = create_calculation(lossbranches)
            losscalculation = CalculationRepository.create(
                session, losscalculation)
            for b in lossbranches:
                b.branch.calculation_oid = losscalculation.oid
                b.branch = CalculationBranchRepository.create(session, b.branch)

        if damagebranches:
            validate_calculation_input(damagebranches)
            damagecalculation = create_calculation(damagebranches)
            damagecalculation = CalculationRepository.create(
                session, damagecalculation)
            for b in damagebranches:
                b.branch.calculation_oid = damagecalculation.oid
                b.branch = CalculationBranchRepository.create(session, b.branch)

        return ((losscalculation, lossbranches),
                (damagecalculation, damagebranches))

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
            working_job = configparser.ConfigParser(interpolation=None)
            working_job.read(str(config))
        elif isinstance(config, configparser.ConfigParser):
            working_job = deepcopy(config)
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
        if working_job['general']['calculation_mode'] == 'scenario_risk':
            working_job.remove_section('fragility')
            for k, v in working_job['vulnerability'].items():
                if k == 'taxonomy_mapping_csv':
                    file = TaxonomyService.export_to_buffer(session, v)
                    file.name = "{}.csv".format(k.replace('_file', ''))
                else:
                    file = VulnerabilityService.export_to_buffer(session, v)
                    file.name = "{}.xml".format(k.replace('_file', ''))
                working_job['vulnerability'][k] = file.name
                calculation_files.append(file)

        elif working_job['general']['calculation_mode'] == 'scenario_damage':
            working_job.remove_section('vulnerability')
            for k, v in working_job['fragility'].items():
                if k == 'taxonomy_mapping_csv':
                    file = TaxonomyService.export_to_buffer(session, v)
                    file.name = "{}.csv".format(k.replace('_file', ''))
                else:
                    file = FragilityService.export_to_buffer(session, v)
                    file.name = "{}.xml".format(k.replace('_file', ''))
                working_job['fragility'][k] = file.name
                calculation_files.append(file)
        else:
            raise ValueError("Unsupported calculation_mode")

        gmfs_config = GMFConfigurationService(working_job)
        gmfs, sites = gmfs_config.get_gmfs(
            exposure_xml, exposure_csv)

        if not working_job.has_section('hazard'):
            working_job.add_section('hazard')
        working_job['hazard']['gmfs_csv'] = gmfs.name
        working_job['hazard']['sites_csv'] = sites.name

        calculation_files.extend([gmfs, sites])

        working_job.remove_section('hazard_source')

        # Generate job configuration file
        job_file = create_file_buffer_configparser(working_job, 'job.ini')
        calculation_files.append(job_file)

        return calculation_files


def add_time(config: configparser.ConfigParser,
             origin_time: datetime) -> configparser.ConfigParser:
    config = deepcopy(config)
    if not config.has_section('risk_calculation'):
        config.add_section('risk_calculation')

    app_settings = get_settings()

    time_event = None

    for period, intervals in app_settings.periods.items():
        if any(start <= origin_time.time() < end for start, end in intervals):
            time_event = period
            break

    if time_event is None:
        raise ValueError("Origin time does not fall within any defined period.")

    config['risk_calculation']['time_event'] = time_event

    return config
