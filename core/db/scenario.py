
import logging

from esloss.datamodel import DamageValue, EStatus, LossValue
from sqlalchemy import insert
from sqlalchemy.orm import Session

from core.db import crud
from core.io.scenario import get_damages, get_losses

LOGGER = logging.getLogger(__name__)


def create_risk_scenario(earthquake_oid: int, aggregation_tags: list,
                         config: dict, session: Session):
    # RISK CALCULATION
    calculation = crud.create_calculation(
        {'aggregateby': ['Canton;CantonGemeinde'],
         'status': EStatus.COMPLETE,
         '_earthquakeinformation_oid': earthquake_oid,
         'calculation_mode': 'scenario_risk'},
        session)

    assert sum([loss['weight'] for loss in config['loss']]) == 1

    for loss_branch in config['loss']:
        LOGGER.info(f'Parsing datastore {loss_branch["store"]}')
        df = get_losses(f'{config["folder"]}/{loss_branch["store"]}')

        df['aggregationtags'] = df['aggregationtags'].apply(
            lambda x: [aggregation_tags[y] for y in x]
        )
        df['weight'] = df['weight'] * loss_branch['weight']
        df['_calculation_oid'] = calculation._oid

        session.execute(insert(LossValue), df.to_dict('records'))

        session.commit()


def create_damage_scenario(earthquake_oid: int, aggregation_tags: list,
                           config: dict, session: Session):
    # RISK CALCULATION
    calculation = crud.create_calculation(
        {'aggregateby': ['Canton;CantonGemeinde'],
         'status': EStatus.COMPLETE,
         '_earthquakeinformation_oid': earthquake_oid,
         'calculation_mode': 'scenario_damage'},
        session)

    assert sum([dmg['weight'] for dmg in config['loss']]) == 1

    for dmg_branch in config['damage']:
        LOGGER.info(f'Parsing datastore {dmg_branch["store"]}')
        df = get_damages(f'{config["folder"]}/{dmg_branch["store"]}')

        df['aggregationtags'] = df['aggregationtags'].apply(
            lambda x: [aggregation_tags[y] for y in x]
        )
        df['weight'] = df['weight'] * dmg_branch['weight']
        df['_calculation_oid'] = calculation._oid

        session.execute(insert(DamageValue), df.to_dict('records'))

        session.commit()
