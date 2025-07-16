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
from reia.schemas.calculation_schemas import (Calculation, CalculationBranch,
                                              DamageCalculation,
                                              DamageCalculationBranch,
                                              EEarthquakeType, LossCalculation,
                                              LossCalculationBranch,
                                              RiskAssessment)


class RiskAssessmentRepository(repository_factory(
        RiskAssessment, RiskAssessmentORM)):
    pass


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
