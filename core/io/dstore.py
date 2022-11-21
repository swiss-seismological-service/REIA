from esloss.datamodel import ELossCategory
from openquake.commonlib.datastore import DataStore
from openquake.risklib.scientific import LOSSTYPE

from core.io import RISK_COLUMNS_MAPPING, ERiskType


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

    if risk_type == ERiskType.DAMAGE:
        loss_types = dstore['oqparam'].loss_types
    else:
        loss_types = LOSSTYPE

    df['losscategory'] = df['losscategory'].map(
        lambda x: ELossCategory[loss_types[x].upper()])

    df['aggregationtags'] = df['aggregationtags'].map(
        all_agg_keys.__getitem__)

    # events have an associated weight which comes from the branch weight
    events['weight'] = events['rlz_id'].map(weights.__getitem__)

    # number of ground motion fields * number of branches
    num_events = len(events)

    df['weight'] = df['eventid'].map(events['weight']) / num_events

    return df