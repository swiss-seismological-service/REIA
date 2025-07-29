from reia.repositories.calculation import (CalculationBranchRepository,
                                           CalculationRepository,
                                           RiskAssessmentRepository)
from reia.repositories.types import SessionType
from reia.schemas.calculation_schemas import (Calculation, CalculationBranch,
                                              RiskAssessment)
from reia.schemas.enums import EStatus
from reia.services.logger import LoggerService


class StatusTracker:
    """Centralized status management with validation and consistent logging."""

    def __init__(self, session: SessionType):
        self.logger = LoggerService.get_logger(__name__)
        self.session = session

    def update_status(
            self,
            entity: RiskAssessment | Calculation | CalculationBranch,
            new_status: EStatus,
            reason: str | None = None):
        """Update entity status with validation and logging.

        Args:
            entity: Entity object (RiskAssessment, Calculation,
                   or CalculationBranch)
            new_status: Target status
            reason: Optional reason for status change

        Returns:
            Updated entity object

        Raises:
            ValueError: If status transition is invalid or entity type unknown
        """
        old_status = entity.status
        self._validate_status_transition(old_status, new_status)

        # Get the appropriate update method based on entity type
        if isinstance(entity, RiskAssessment):
            update_method = \
                RiskAssessmentRepository.update_risk_assessment_status
            entity_type_name = "RiskAssessment"
        elif isinstance(entity, CalculationBranch):
            update_method = CalculationBranchRepository.update_status
            entity_type_name = "CalculationBranch"
        elif isinstance(entity, Calculation):
            update_method = CalculationRepository.update_status
            entity_type_name = "Calculation"
        else:
            raise ValueError(f"Unknown entity type: {type(entity)}")

        # Update status using the appropriate repository method
        updated_entity = update_method(self.session, entity.oid, new_status)

        # Log the status change
        reason_msg = f" ({reason})" if reason else ""
        self.logger.info(
            f"{entity_type_name} {entity.oid} status changed: "
            f"{old_status.name} → {new_status.name}{reason_msg}")

        return updated_entity

    def _validate_status_transition(
            self,
            old_status: EStatus,
            new_status: EStatus) -> None:
        """Validate that a status transition is allowed.

        Args:
            old_status: Current status
            new_status: Target status

        Raises:
            ValueError: If transition is not allowed
        """
        # Define valid transitions
        valid_transitions = {
            EStatus.CREATED: {
                EStatus.EXECUTING, EStatus.COMPLETE,
                EStatus.FAILED, EStatus.ABORTED},
            EStatus.EXECUTING: {
                EStatus.COMPLETE, EStatus.FAILED, EStatus.ABORTED},
            # Terminal states cannot transition
            EStatus.COMPLETE: set(),
            EStatus.FAILED: set(),
            EStatus.ABORTED: set(),
        }

        # Allow staying in the same status (idempotent updates)
        if old_status == new_status:
            return

        allowed_statuses = valid_transitions.get(old_status, set())
        if new_status not in allowed_statuses:
            raise ValueError(
                f"Invalid status transition: {old_status.name} → "
                f"{new_status.name}. Valid transitions from "
                f"{old_status.name} are: {[s.name for s in allowed_statuses]}")

    def validate_risk_assessment_completion(
            self,
            loss_calculation,
            damage_calculation) -> EStatus:
        """Determine appropriate risk assessment status based on calculations.

        Args:
            loss_calculation: Loss calculation object
            damage_calculation: Damage calculation object

        Returns:
            Appropriate status for the risk assessment
        """
        if (loss_calculation.status == EStatus.COMPLETE
                and damage_calculation.status == EStatus.COMPLETE):
            return EStatus.COMPLETE
        elif (loss_calculation.status in [EStatus.FAILED, EStatus.ABORTED]
              or damage_calculation.status in [
                  EStatus.FAILED, EStatus.ABORTED]):
            return EStatus.FAILED
        else:
            # Still executing
            return EStatus.EXECUTING

    def validate_calculation_completion(self, branches: list) -> EStatus:
        """Determine appropriate calculation status based on branches.

        Args:
            branches: List of calculation branch objects

        Returns:
            Appropriate status for the calculation
        """
        if all(b.status == EStatus.COMPLETE for b in branches):
            return EStatus.COMPLETE
        elif any(b.status in [EStatus.FAILED, EStatus.ABORTED]
                 for b in branches):
            return EStatus.FAILED
        else:
            # Still executing
            return EStatus.EXECUTING
