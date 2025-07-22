import os

from openquake.commonlib.datastore import DataStore
from openquake.risklib.scientific import LOSSTYPE

from reia.datamodel import ELossCategory
from reia.io import RISK_COLUMNS_MAPPING
from reia.schemas.enums import ERiskType


def get_risk_from_dstore(dstore: DataStore, risk_type: ERiskType):

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

    if int(os.getenv('OQ_VERSION', '15')) >= 15:
        loss_types = LOSSTYPE
    else:
        loss_types = dstore['oqparam'].loss_types

    df['losscategory'] = df['losscategory'].map(
        lambda x: ELossCategory[loss_types[x].upper()])

    df['aggregationtags'] = df['aggregationtags'].map(
        all_agg_keys.__getitem__)

    # events have an associated weight which comes from the branch weight
    events['weight'] = events['rlz_id'].map(weights.__getitem__)

    df['weight'] = df['eventid'].map(
        events['weight']) / dstore['oqparam'].number_of_ground_motion_fields

    if risk_type == ERiskType.DAMAGE:
        df = df[(df['dg1_value'] > 0)
                | (df['dg2_value'] > 0)
                | (df['dg3_value'] > 0)
                | (df['dg4_value'] > 0)
                | (df['dg5_value'] > 0)]

    return df
