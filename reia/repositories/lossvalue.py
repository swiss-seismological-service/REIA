from reia.datamodel.lossvalues import DamageValue as DamageValueORM
from reia.datamodel.lossvalues import LossValue as LossValueORM
from reia.datamodel.lossvalues import RiskValue as RiskValueORM
from reia.repositories.base import repository_factory
from reia.schemas.lossvalue_schemas import DamageValue, LossValue, RiskValue


class RiskValueRepository(repository_factory(RiskValue, RiskValueORM)):
    pass


class LossValueRepository(repository_factory(LossValue, LossValueORM)):
    pass


class DamageValueRepository(repository_factory(DamageValue, DamageValueORM)):
    pass
