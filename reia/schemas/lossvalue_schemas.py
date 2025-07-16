from typing import Optional

from pydantic import Field

from reia.schemas.base import Model, real_value_mixin
from reia.schemas.calculation_schemas import ECalculationType
from reia.schemas.vulnerability_schemas import ELossCategory


class RiskValue(Model):
    oid: Optional[int] = Field(default=None, alias='_oid')
    type: Optional[ECalculationType] = Field(default=None, alias='_type')
    losscategory: Optional[ELossCategory] = None
    eventid: Optional[int] = None
    weight: Optional[float] = None
    calculation_oid: Optional[int] = Field(
        default=None, alias='_calculation_oid')
    calculationbranch_oid: Optional[int] = Field(
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
