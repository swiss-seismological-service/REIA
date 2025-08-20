from sqlalchemy import select, true
from sqlalchemy.orm import Session

from reia.datamodel.calculations import Calculation as CalculationORM
from reia.datamodel.calculations import \
    CalculationBranch as CalculationBranchORM
from reia.datamodel.calculations import \
    DamageCalculation as DamageCalculationORM
from reia.datamodel.calculations import \
    DamageCalculationBranch as DamageCalculationBranchORM
from reia.datamodel.calculations import LossCalculation as LossCalculationORM
from reia.datamodel.calculations import \
    LossCalculationBranch as LossCalculationBranchORM
from reia.datamodel.calculations import RiskAssessment as RiskAssessmentORM
from reia.repositories.base import repository_factory
from reia.repositories.utils import drop_dynamic_table, drop_partition_table
from reia.schemas.calculation_schemas import (Calculation, CalculationBranch,
                                              DamageCalculation,
                                              DamageCalculationBranch,
                                              LossCalculation,
                                              LossCalculationBranch,
                                              RiskAssessment)
from reia.schemas.enums import ECalculationType, EStatus
from reia.services.logger import LoggerService

logger = LoggerService.get_logger(__name__)


class RiskAssessmentRepository(repository_factory(
        RiskAssessment, RiskAssessmentORM)):
    @classmethod
    def delete(cls, session: Session, riskassessment_oid: int) -> int:
        logger.info(f"Deleting risk assessment {riskassessment_oid}")
        # Fetch the risk assessment
        riskassessment = RiskAssessmentRepository.get_by_id(
            session, riskassessment_oid)

        if not riskassessment:
            logger.warning(f"Risk assessment {riskassessment_oid} not found")
            return 0

        losscalc_oid = riskassessment.losscalculation_oid
        damagecalc_oid = riskassessment.damagecalculation_oid

        # Delete the RiskAssessment entry itself
        super().delete(session, riskassessment_oid)

        bind = session.get_bind()

        # Handle loss calculation
        if losscalc_oid:
            logger.debug(f"Dropping loss calculation tables for {losscalc_oid}")
            drop_dynamic_table(bind, f"loss_assoc_{losscalc_oid}")
            drop_partition_table(bind, "loss_riskvalue", losscalc_oid)
            LossCalculationRepository.delete(session, losscalc_oid)

        # Handle damage calculation
        if damagecalc_oid:
            logger.debug(
                f"Dropping damage calculation tables for {damagecalc_oid}")
            drop_dynamic_table(bind, f"loss_assoc_{damagecalc_oid}")
            drop_partition_table(bind, "loss_riskvalue", damagecalc_oid)
            DamageCalculationRepository.delete(session, damagecalc_oid)

        session.commit()
        session.remove()
        logger.info(
            f"Successfully deleted risk assessment {riskassessment_oid}")
        return 1

    @classmethod
    def update_risk_assessment_status(
            cls, session: Session, riskassessment_oid: int,
            status: EStatus) -> RiskAssessment:
        risk_assessment = cls.get_by_id(session, riskassessment_oid)
        if not risk_assessment:
            raise ValueError(
                f"Risk assessment with OID {riskassessment_oid} not found.")

        risk_assessment.status = status

        return cls.update(session, risk_assessment)


class CalculationBranchRepository(repository_factory(
        CalculationBranch, CalculationBranchORM)):
    @classmethod
    def create(cls,
               session: Session,
               data: CalculationBranch) -> CalculationBranch:
        if data.type == ECalculationType.LOSS:
            return LossCalculationBranchRepository.create(session, data)
        elif data.type == ECalculationType.DAMAGE:
            return DamageCalculationBranchRepository.create(session, data)
        else:
            raise ValueError(f"Unsupported calculation type: {data.type}")

    @classmethod
    def get_by_id(cls, session: Session, oid: int) -> CalculationBranch:
        branch = super().get_by_id(session, oid)
        if branch.type == ECalculationType.LOSS:
            return LossCalculationBranchRepository.get_by_id(session, oid)
        elif branch.type == ECalculationType.DAMAGE:
            return DamageCalculationBranchRepository.get_by_id(session, oid)
        else:
            raise ValueError(f"Unsupported calculation type: {branch.type}")

    @classmethod
    def update_status(cls, session: Session, oid: int,
                      status: EStatus) -> CalculationBranch:
        branch = super().get_by_id(session, oid)
        branch.status = status
        cls.update(session, branch)
        return branch


class LossCalculationBranchRepository(repository_factory(
        LossCalculationBranch, LossCalculationBranchORM)):
    pass


class DamageCalculationBranchRepository(repository_factory(
        DamageCalculationBranch, DamageCalculationBranchORM)):
    pass


class CalculationRepository(repository_factory(
        Calculation, CalculationORM)):
    @classmethod
    def create(cls, session: Session, data: Calculation) -> Calculation:
        if data.type == ECalculationType.LOSS:
            return LossCalculationRepository.create(session, data)
        elif data.type == ECalculationType.DAMAGE:
            return DamageCalculationRepository.create(session, data)
        else:
            raise ValueError(f"Unsupported calculation type: {data.type}")

    @classmethod
    def get_by_id(cls, session: Session, oid: int) -> Calculation:
        calc = super().get_by_id(session, oid)
        if calc.type == ECalculationType.LOSS:
            return LossCalculationRepository.get_by_id(session, oid)
        elif calc.type == ECalculationType.DAMAGE:
            return DamageCalculationRepository.get_by_id(session, oid)

    @classmethod
    def get_all_by_type(
            cls,
            session: Session,
            type: ECalculationType | None = None) -> list[Calculation]:

        stmt = select(CalculationORM).where(
            CalculationORM._type == type if type else true()
        )
        result = session.execute(stmt).unique().scalars().all()
        return [Calculation.model_validate(row) for row in result]

    @classmethod
    def update_status(
            cls, session: Session, oid: int, status: EStatus) -> Calculation:
        calc = super().get_by_id(session, oid)
        calc.status = status
        cls.update(session, calc)
        return calc


class LossCalculationRepository(repository_factory(
        LossCalculation, LossCalculationORM)):
    pass


class DamageCalculationRepository(repository_factory(
        DamageCalculation, DamageCalculationORM)):
    pass
