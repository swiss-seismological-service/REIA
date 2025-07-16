from typing import Optional

from reia.schemas.base import Model, real_value_mixin
from reia.schemas.calculation_schemas import ECalculationType
from reia.schemas.vulnerability_schemas import ELossCategory


class RiskValue(Model):
    _oid: Optional[int] = None
    _type: Optional[ECalculationType] = None
    losscategory: Optional[ELossCategory] = None
    eventid: Optional[int] = None
    weight: Optional[float] = None
    _calculation_oid: Optional[int] = None
    _calculationbranch_oid: Optional[int] = None


class LossValue(
    RiskValue,
    real_value_mixin('loss', float)
):
    pass


class DamageValue(
    RiskValue,
    real_value_mixin('dg1', float),
    real_value_mixin('dg2', float),
    real_value_mixin('dg3', float),
    real_value_mixin('dg4', float),
    real_value_mixin('dg5', float)
):
    pass
