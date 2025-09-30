from pathlib import Path

from reia.repositories.calculation import RiskAssessmentRepository
from reia.schemas.calculation_schemas import RiskAssessment
from reia.schemas.enums import EEarthquakeType, EStatus
from reia.services.calculation import (CalculationDataService,
                                       CalculationExecutionService)
from reia.services.creation_info import populate_creation_info
from reia.services.logger import LoggerService
from reia.services.status_tracker import StatusTracker


class RiskAssessmentService:
    """Service for managing risk assessment workflows."""

    def __init__(self, session):
        self.logger = LoggerService.get_logger(__name__)

        self.session = session
        self.status_tracker = StatusTracker(session)

    def run_risk_assessment(self,
                            originid: str,
                            configs: list[Path],
                            weights: list[float]) -> RiskAssessment:
        """Run a complete risk assessment with loss and damage calculations.

        Args:
            originid: Unique identifier for the risk assessment
            configs:
            weights:

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

        # Populate creation info with system values
        populate_creation_info(risk_assessment)
        risk_assessment = RiskAssessmentRepository.create(
            self.session, risk_assessment)

        # Update status to executing
        risk_assessment = self.status_tracker.update_status(
            risk_assessment,
            EStatus.EXECUTING,
            "Starting risk assessment processing")

        loss, damage = CalculationDataService.import_from_files(
            self.session, configs, weights)

        calc_service = CalculationExecutionService(self.session)

        try:
            # run loss calculations
            loss_calculation, loss_branches = loss
            if loss_calculation is not None:
                self.logger.info(f"Running loss calculation for "
                                 f"risk assessment {risk_assessment.oid} "
                                 f"with {len(loss_branches)} branches.")
                loss_calculation = calc_service.run_calculations(
                    loss_calculation, loss_branches)
                risk_assessment.losscalculation_oid = loss_calculation.oid
                risk_assessment = RiskAssessmentRepository.update(
                    self.session, risk_assessment)
                self.logger.info("Loss calculation completed with "
                                 f"status: {loss_calculation.status.name}")

            # run damage calculations
            damage_calculation, damage_branches = damage
            if damage_calculation is not None:
                self.logger.info(f"Running damage calculation for "
                                 f"risk assessment {risk_assessment.oid} "
                                 f"with {len(damage_branches)} branches.")
                damage_calculation = calc_service.run_calculations(
                    damage_calculation, damage_branches)
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

        except Exception as e:
            # Handle failures and keyboard interrupts
            status = EStatus.ABORTED if isinstance(
                e, KeyboardInterrupt) else EStatus.FAILED

            # Log the error with context
            self.logger.error(f"Risk assessment {originid} failed: {str(e)}",
                              exc_info=True)

            self.status_tracker.update_status(risk_assessment,
                                              status,
                                              f"Exception occurred: {str(e)}")
            raise e
