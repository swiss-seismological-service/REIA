import configparser
from pathlib import Path

from reia.repositories.calculation import RiskAssessmentRepository
from reia.schemas.calculation_schemas import (CalculationBranchSettings,
                                              RiskAssessment)
from reia.schemas.enums import EEarthquakeType, EStatus
from reia.services.calculation import CalculationService
from reia.services.logger import LoggerService
from reia.services.status_tracker import StatusTracker


class RiskAssessmentService:
    """Service for managing risk assessment workflows."""

    def __init__(self, session):
        # Initialize logging for risk assessment workflows
        LoggerService.setup_logging()
        self.logger = LoggerService.get_logger(__name__)

        self.session = session
        self.status_tracker = StatusTracker(session)

    def run_risk_assessment(self, originid: str, loss_config_path: str,
                            damage_config_path: str) -> RiskAssessment:
        """Run a complete risk assessment with loss and damage calculations.

        Args:
            originid: Unique identifier for the risk assessment
            loss_config_path: Path to loss calculation configuration file
            damage_config_path: Path to damage calculation configuration file

        Returns:
            Created RiskAssessment object

        Raises:
            Exception: If calculations or risk assessment creation fails
        """
        # Create initial risk assessment record
        self.logger.info(f"Starting risk assessment workflow for {originid}")

        risk_assessment = RiskAssessment(
            originid=originid,
            type=EEarthquakeType.NATURAL,
            status=EStatus.CREATED
        )
        risk_assessment = RiskAssessmentRepository.create(
            self.session, risk_assessment)

        try:
            # Update status to executing
            risk_assessment = self.status_tracker.update_status(
                risk_assessment,
                EStatus.EXECUTING,
                "Starting risk assessment processing")

            # Run loss calculation
            self.logger.info("Starting loss calculation for risk "
                             f"assessment {risk_assessment.oid}")
            loss_calculation = self._run_loss_calculation(loss_config_path)
            risk_assessment.losscalculation_oid = loss_calculation.oid
            risk_assessment = RiskAssessmentRepository.update(
                self.session, risk_assessment)
            self.logger.info("Loss calculation completed with "
                             f"status: {loss_calculation.status.name}")

            # Run damage calculation
            self.logger.info("Starting damage calculation for "
                             f"risk assessment {risk_assessment.oid}")
            damage_calculation = self._run_damage_calculation(
                damage_config_path)
            risk_assessment.damagecalculation_oid = damage_calculation.oid
            risk_assessment = RiskAssessmentRepository.update(
                self.session, risk_assessment)
            self.logger.info("Damage calculation completed with "
                             f"status: {damage_calculation.status.name}")

            # Determine final status based on calculation results
            final_status = \
                self.status_tracker.validate_risk_assessment_completion(
                    loss_calculation, damage_calculation)

            risk_assessment = self.status_tracker.update_status(
                risk_assessment,
                final_status,
                "Risk assessment calculations completed")

            self.logger.info(f"Risk assessment {originid} completed with "
                             f"final status: {final_status.name}")
            return risk_assessment

        except BaseException as e:
            # Handle failures and keyboard interrupts
            status = EStatus.ABORTED if isinstance(
                e, KeyboardInterrupt) else EStatus.FAILED

            # Log the error with context
            self.logger.error(f"Risk assessment {originid} failed: {str(e)}",
                              exc_info=True)

            self.status_tracker.update_status(risk_assessment,
                                              status,
                                              f"Exception occurred: {str(e)}")
            raise

    def _run_loss_calculation(self, config_path: str):
        """Run loss calculation from config file."""
        job_file = configparser.ConfigParser()
        job_file.read(Path(config_path))
        branch_settings = CalculationBranchSettings(weight=1, config=job_file)

        calc_service = CalculationService(self.session)
        return calc_service.run_calculations([branch_settings])

    def _run_damage_calculation(self, config_path: str):
        """Run damage calculation from config file."""
        job_file = configparser.ConfigParser()
        job_file.read(Path(config_path))
        branch_settings = CalculationBranchSettings(weight=1, config=job_file)

        calc_service = CalculationService(self.session)
        return calc_service.run_calculations([branch_settings])
