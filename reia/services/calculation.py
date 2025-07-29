from sqlalchemy.orm import Session

from reia.api import OQCalculationAPI
from reia.io.read import parse_calculation_input, validate_calculation_input
from reia.io.write import assemble_calculation_input
from reia.repositories.calculation import (CalculationBranchRepository,
                                           CalculationRepository)
from reia.schemas.calculation_schemas import CalculationBranchSettings
from reia.schemas.enums import EStatus
from reia.services.logger import LoggerService
from reia.services.results import ResultsService
from reia.services.status_tracker import StatusTracker
from settings import get_config


class CalculationService:
    """Service for managing OpenQuake calculations."""

    def __init__(self, session: Session):
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
        # Validate inputs
        self.logger.info("Starting calculation workflow with "
                         f"{len(branch_settings)} branches")
        validate_calculation_input(branch_settings)

        # Parse calculation information
        calculation_dict, branches_dicts = parse_calculation_input(
            branch_settings)

        # Create calculation and branches in database
        calculation = CalculationRepository.create(self.session,
                                                   calculation_dict)
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

    def _run_single_calculation(
            self,
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
        files = assemble_calculation_input(setting.config, self.session)
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
