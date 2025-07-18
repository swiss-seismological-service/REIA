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
from reia.db.copy import drop_dynamic_table, drop_partition_table
from reia.repositories.base import repository_factory
from reia.schemas.calculation_schemas import (Calculation, CalculationBranch,
                                              DamageCalculation,
                                              DamageCalculationBranch,
                                              EEarthquakeType, LossCalculation,
                                              LossCalculationBranch,
                                              RiskAssessment)


class RiskAssessmentRepository(repository_factory(
        RiskAssessment, RiskAssessmentORM)):
    @classmethod
    def delete(cls, session: Session, riskassessment_oid: int) -> int:
        # Fetch the risk assessment
        riskassessment = RiskAssessmentRepository.get_by_id(
            session, riskassessment_oid)

        if not riskassessment:
            return 0

        losscalc_oid = riskassessment.losscalculation_oid
        damagecalc_oid = riskassessment.damagecalculation_oid

        # Delete the RiskAssessment entry itself
        RiskAssessmentRepository.delete(session, riskassessment_oid)

        bind = session.get_bind()

        # Handle loss calculation
        if losscalc_oid:
            drop_dynamic_table(bind, f"loss_assoc_{losscalc_oid}")
            drop_partition_table(bind, "loss_riskvalue", losscalc_oid)
            LossCalculationRepository.delete(session, losscalc_oid)

        # Handle damage calculation
        if damagecalc_oid:
            drop_dynamic_table(bind, f"loss_assoc_{damagecalc_oid}")
            drop_partition_table(bind, "loss_riskvalue", damagecalc_oid)
            DamageCalculationRepository.delete(session, damagecalc_oid)

        session.commit()
        session.remove()
        return 1


class CalculationBranchRepository(repository_factory(
        CalculationBranch, CalculationBranchORM)):
    pass


class LossCalculationBranchRepository(repository_factory(
        LossCalculationBranch, LossCalculationBranchORM)):
    pass


class DamageCalculationBranchRepository(repository_factory(
        DamageCalculationBranch, DamageCalculationBranchORM)):
    pass


class CalculationRepository(repository_factory(
        Calculation, CalculationORM)):
    @classmethod
    def get_all_by_type(
            session: Session,
            type: EEarthquakeType | None = None) -> list[Calculation]:

        stmt = select(CalculationORM).where(
            CalculationORM._earthquake_type == type if type else true()
        )
        result = session.execute(stmt).unique().scalars().all()
        return [Calculation.model_validate(row) for row in result]


class LossCalculationRepository(repository_factory(
        LossCalculation, LossCalculationORM)):
    pass


class DamageCalculationRepository(repository_factory(
        DamageCalculation, DamageCalculationORM)):
    pass
