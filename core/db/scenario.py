
import logging
from io import StringIO

import pandas as pd
import psycopg2
from esloss.datamodel import EStatus, RiskValue, riskvalue_aggregationtag
from sqlalchemy.orm import Session

from core.db import crud, engine
from core.io.scenario import get_risk_from_dstore

LOGGER = logging.getLogger(__name__)


def copy_from_dataframe(cursor, df: pd.DataFrame, table: str):
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, header=False, index=False)
    buffer.seek(0)

    try:
        cursor.copy_from(buffer, table, sep=",", columns=df.columns)
        LOGGER.info("Data inserted successfully....")
    except (Exception, psycopg2.DatabaseError) as err:
        LOGGER.error(err)
        cursor.close()


def get_nextval(cursor, table: str, column: str):
    # set sequence to correct number
    cursor.execute(
        f"SELECT setval(pg_get_serial_sequence('{table}', '{column}'), "
        f"coalesce(max({column}),0) + 1, false) FROM {table};"
    )
    # get nextval
    cursor.execute(
        f"select nextval(pg_get_serial_sequence('{table}', '{column}'))")
    next = cursor.fetchone()[0]
    return next


def create_risk_scenario(earthquake_oid: int, aggregation_tags: list,
                         config: dict, session: Session):

    assert sum([loss['weight'] for loss in config['loss']]) == 1

    calculation = crud.create_calculation(
        {'aggregateby': ['Canton;CantonGemeinde'],
         'status': EStatus.COMPLETE,
         '_earthquakeinformation_oid': earthquake_oid,
         'calculation_mode': 'scenario_risk'},
        session)

    select_columns = {'event_id': 'eventid',
                      'agg_id': 'aggregationtags',
                      'loss_id': 'losscategory',
                      'loss': 'loss_value'}

    connection = engine.raw_connection()

    for loss_branch in config['loss']:
        LOGGER.info(f'Parsing datastore {loss_branch["store"]}')

        dstore_path = f'{config["folder"]}/{loss_branch["store"]}'

        df = get_risk_from_dstore(dstore_path, select_columns)

        df['aggregationtags'] = df['aggregationtags'].apply(
            lambda x: [aggregation_tags[y]._oid for y in x]
        )
        df['losscategory'] = df['losscategory'].apply(lambda x: x.name)
        df['weight'] = df['weight'] * loss_branch['weight']
        df['_calculation_oid'] = calculation._oid
        df['_type'] = 'lossvalue'

        cursor = connection.cursor()
        cursor.execute(
            f'LOCK TABLE {RiskValue.__table__.name} IN EXCLUSIVE MODE;')

        index0 = get_nextval(cursor, RiskValue.__table__.name, '_oid')
        df['_oid'] = range(index0, index0 + len(df))

        # build up reference table
        df_agg_val = pd.DataFrame(
            {'riskvalue': df['_oid'],
             'aggregationtag': df.pop('aggregationtags')})
        df_agg_val = df_agg_val.explode('aggregationtag', ignore_index=True)

        copy_from_dataframe(cursor, df, RiskValue.__table__.name)
        copy_from_dataframe(
            cursor, df_agg_val, riskvalue_aggregationtag.name)
        connection.commit()
        cursor.close()

    connection.close()


def create_damage_scenario(earthquake_oid: int, aggregation_tags: list,
                           config: dict, session: Session):

    assert sum([dmg['weight'] for dmg in config['loss']]) == 1

    calculation = crud.create_calculation(
        {'aggregateby': ['Canton;CantonGemeinde'],
         'status': EStatus.COMPLETE,
         '_earthquakeinformation_oid': earthquake_oid,
         'calculation_mode': 'scenario_damage'},
        session)

    select_columns = {'event_id': 'eventid',
                      'agg_id': 'aggregationtags',
                      'loss_id': 'losscategory',
                      'dmg_1': 'dg1_value',
                      'dmg_2': 'dg2_value',
                      'dmg_3': 'dg3_value',
                      'dmg_4': 'dg4_value',
                      'dmg_5': 'dg5_value', }

    connection = engine.raw_connection()

    for dmg_branch in config['damage']:
        LOGGER.info(f'Parsing datastore {dmg_branch["store"]}')

        dstore_path = f'{config["folder"]}/{dmg_branch["store"]}'

        df = get_risk_from_dstore(dstore_path, select_columns)

        df['aggregationtags'] = df['aggregationtags'].apply(
            lambda x: [aggregation_tags[y]._oid for y in x]
        )
        df['weight'] = df['weight'] * dmg_branch['weight']
        df['_calculation_oid'] = calculation._oid
        df['losscategory'] = df['losscategory'].apply(lambda x: x.name)
        df['_type'] = 'damagevalue'

        cursor = connection.cursor()
        cursor.execute(
            f'LOCK TABLE {RiskValue.__table__.name} IN EXCLUSIVE MODE;')

        index0 = get_nextval(cursor, RiskValue.__table__.name, '_oid')
        df['_oid'] = range(index0, index0 + len(df))

        # build up reference table
        df_agg_val = pd.DataFrame(
            {'riskvalue': df['_oid'],
             'aggregationtag': df.pop('aggregationtags')})
        df_agg_val = df_agg_val.explode('aggregationtag', ignore_index=True)

        copy_from_dataframe(cursor, df, RiskValue.__table__.name)
        copy_from_dataframe(
            cursor, df_agg_val, riskvalue_aggregationtag.name)
        connection.commit()
        cursor.close()

    connection.close()
