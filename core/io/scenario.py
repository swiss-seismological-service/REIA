import enum

import pandas as pd
from esloss.datamodel import ELossCategory
from openquake.commonlib.datastore import read

from core.io.parse_input import parse_exposure


class ERiskType(str, enum.Enum):
    LOSS = 'scenario_risk'
    DAMAGE = 'scenario_damage'


RISK_COLUMNS_MAPPING = {
    ERiskType.LOSS: {'event_id': 'eventid',
                     'agg_id': 'aggregationtags',
                     'loss_id': 'losscategory',
                     'loss': 'loss_value'},
    ERiskType.DAMAGE: {'event_id': 'eventid',
                       'agg_id': 'aggregationtags',
                       'loss_id': 'losscategory',
                       'dmg_1': 'dg1_value',
                       'dmg_2': 'dg2_value',
                       'dmg_3': 'dg3_value',
                       'dmg_4': 'dg4_value',
                       'dmg_5': 'dg5_value', }}


def get_risk_from_dstore(path: str, risk_type: ERiskType):
    dstore = read(path)

    all_agg_keys = [d.decode().split(',')
                    for d in dstore['agg_keys'][:]]

    df = dstore.read_df('risk_by_event')  # get risk_by_event

    weights = dstore['weights'][:]
    events = dstore.read_df('events', 'id')[['rlz_id']]

    # risk by event contains more agg_id's than keys which
    # are used to store the total per agg value. Remove them.
    df = df.loc[df['agg_id'] != len(all_agg_keys)]
    cols_mapping = RISK_COLUMNS_MAPPING[risk_type]
    df = df.rename(columns=cols_mapping)[cols_mapping.values()]

    lti = {v: k for k, v in dstore['oqparam'].lti.items()}

    df['losscategory'] = df['losscategory'].map(
        lambda x: ELossCategory[lti[x].upper()])

    df['aggregationtags'] = df['aggregationtags'].map(
        all_agg_keys.__getitem__)

    # events have an associated weight which comes from the branch weight
    events['weight'] = events['rlz_id'].map(weights.__getitem__)

    # number of ground motion fields * number of branches
    num_events = len(events)

    df['weight'] = df['eventid'].map(events['weight']) / num_events

    return df


def combine_assetfiles(files: list[str]) -> pd.DataFrame():
    asset_collection = pd.DataFrame()

    for exposure in files:
        with open(exposure, 'r') as f:
            _, assets = parse_exposure(f)
            asset_collection = pd.concat([asset_collection, assets])
    return asset_collection
