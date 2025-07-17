
from pydantic import Field

from reia.schemas.base import Model, real_value_mixin
from reia.schemas.calculation_schemas import ECalculationType
from reia.schemas.vulnerability_schemas import ELossCategory


class RiskValue(Model):
    oid: int | None = Field(default=None, alias='_oid')
    type: ECalculationType | None = Field(default=None, alias='_type')
    losscategory: ELossCategory | None = None
    eventid: int | None = None
    weight: float | None = None
    calculation_oid: int | None = Field(
        default=None, alias='_calculation_oid')
    calculationbranch_oid: int | None = Field(
        default=None, alias='_calculationbranch_oid')


class LossValue(RiskValue,
                real_value_mixin('loss', float)):
    pass


class DamageValue(RiskValue,
                  real_value_mixin('dg1', float),
                  real_value_mixin('dg2', float),
                  real_value_mixin('dg3', float),
                  real_value_mixin('dg4', float),
                  real_value_mixin('dg5', float)):
    pass
