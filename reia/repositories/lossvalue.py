import pandas as pd
from sqlalchemy.orm import Session

from reia.datamodel.lossvalues import DamageValue as DamageValueORM
from reia.datamodel.lossvalues import LossValue as LossValueORM
from reia.datamodel.lossvalues import RiskValue as RiskValueORM
from reia.datamodel.lossvalues import riskvalue_aggregationtag
from reia.db.copy import allocate_oids, copy_pooled, db_cursor_from_session
from reia.repositories.base import repository_factory
from reia.schemas.lossvalue_schemas import DamageValue, LossValue, RiskValue


class RiskValueRepository(repository_factory(RiskValue, RiskValueORM)):
    @classmethod
    def insert_many(cls,
                    session: Session,
                    risk_values: pd.DataFrame,
                    df_agg_val: pd.DataFrame) -> None:
        with db_cursor_from_session(session) as cursor:
            db_indexes = allocate_oids(cursor,
                                       RiskValueORM.__table__.name,
                                       '_oid',
                                       len(risk_values))

        oid_map = dict(zip(risk_values['_oid'], db_indexes))

        risk_values['_oid'] = db_indexes
        df_agg_val['riskvalue'] = df_agg_val['riskvalue'].map(oid_map)

        copy_pooled(risk_values, RiskValueORM.__table__.name)
        copy_pooled(df_agg_val, riskvalue_aggregationtag.name)


class LossValueRepository(repository_factory(LossValue, LossValueORM)):
    pass


class DamageValueRepository(repository_factory(DamageValue, DamageValueORM)):
    pass
