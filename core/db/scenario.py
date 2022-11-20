
import logging
from operator import attrgetter

from esloss.datamodel import EStatus
from sqlalchemy.orm import Session

from core.db import crud, engine
from core.io.scenario import ERiskType, get_risk_from_dstore

LOGGER = logging.getLogger(__name__)


def create_risk_scenario(earthquake_oid: int,
                         risk_type: ERiskType,
                         aggregation_tags: list,
                         config: dict,
                         session: Session):

    assert sum([loss['weight']
               for loss in config[risk_type.name.lower()]]) == 1

    calculation = crud.create_calculation(
        {'aggregateby': ['Canton;CantonGemeinde'],
         'status': EStatus.COMPLETE,
         '_earthquakeinformation_oid': earthquake_oid,
         'calculation_mode': risk_type.value},
        session)

    connection = engine.raw_connection()

    for loss_branch in config[risk_type.name.lower()]:
        LOGGER.info(f'Parsing datastore {loss_branch["store"]}')

        dstore_path = f'{config["folder"]}/{loss_branch["store"]}'

        df = get_risk_from_dstore(dstore_path, risk_type)

        df['losscategory'] = df['losscategory'].map(attrgetter('name'))
        df['weight'] = df['weight'] * loss_branch['weight']
        df['_calculation_oid'] = calculation._oid
        df['_type'] = f'{risk_type.name.lower()}value'

        crud.create_risk_values(df, aggregation_tags, connection)
        break
    connection.close()
