import configparser
import traceback
from pathlib import Path

from reia.repositories.calculation import RiskAssessmentRepository
from reia.schemas.calculation_schemas import (CalculationBranchSettings,
                                              RiskAssessment)
from reia.schemas.enums import EEarthquakeType, EStatus
from reia.services.calculation import CalculationService


class RiskAssessmentService:
    """Service for managing risk assessment workflows."""

    def __init__(self, session):
        self.session = session

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
            risk_assessment = \
                RiskAssessmentRepository.update_risk_assessment_status(
                    self.session, risk_assessment.oid, EStatus.EXECUTING)

            # Run loss calculation
            loss_calculation = self._run_loss_calculation(loss_config_path)
            risk_assessment.losscalculation_oid = loss_calculation.oid

            # Run damage calculation
            damage_calculation = self._run_damage_calculation(
                damage_config_path)
            risk_assessment.damagecalculation_oid = damage_calculation.oid

            # Determine final status based on calculation results
            final_status = EStatus.COMPLETE if all([
                loss_calculation.status == EStatus.COMPLETE,
                damage_calculation.status == EStatus.COMPLETE
            ]) else EStatus.FAILED

            risk_assessment.status = final_status
            risk_assessment = RiskAssessmentRepository.update(
                self.session, risk_assessment)

            return risk_assessment

        except BaseException as e:
            # Handle failures and keyboard interrupts
            status = EStatus.ABORTED if isinstance(
                e, KeyboardInterrupt) else EStatus.FAILED
            RiskAssessmentRepository.update_risk_assessment_status(
                self.session, risk_assessment.oid, status)
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
