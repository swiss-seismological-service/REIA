import configparser
import traceback
from pathlib import Path

from reia.repositories.calculation import RiskAssessmentRepository
from reia.schemas.calculation_schemas import (CalculationBranchSettings,
                                              RiskAssessment)
from reia.schemas.enums import EEarthquakeType, EStatus
from reia.services.calculation import CalculationService
from reia.services.status_tracker import StatusTracker


class RiskAssessmentService:
    """Service for managing risk assessment workflows."""

    def __init__(self, session):
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
            loss_calculation = self._run_loss_calculation(loss_config_path)
            risk_assessment.losscalculation_oid = loss_calculation.oid
            risk_assessment = RiskAssessmentRepository.update(
                self.session, risk_assessment)

            # Run damage calculation
            damage_calculation = self._run_damage_calculation(
                damage_config_path)
            risk_assessment.damagecalculation_oid = damage_calculation.oid
            risk_assessment = RiskAssessmentRepository.update(
                self.session, risk_assessment)

            # Determine final status based on calculation results
            final_status = \
                self.status_tracker.validate_risk_assessment_completion(
                    loss_calculation, damage_calculation)

            risk_assessment = self.status_tracker.update_status(
                risk_assessment,
                final_status,
                "Risk assessment calculations completed")

            return risk_assessment

        except BaseException as e:
            # Handle failures and keyboard interrupts
            status = EStatus.ABORTED if isinstance(
                e, KeyboardInterrupt) else EStatus.FAILED
            self.status_tracker.update_status(risk_assessment,
                                              status,
                                              f"Exception occurred: {str(e)}")
            traceback.print_exc()
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
