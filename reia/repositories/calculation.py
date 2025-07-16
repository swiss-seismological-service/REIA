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
                                              LossCalculation,
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
    pass


class LossCalculationRepository(repository_factory(
        LossCalculation, LossCalculationORM)):
    pass


class DamageCalculationRepository(repository_factory(
        DamageCalculation, DamageCalculationORM)):
    pass
