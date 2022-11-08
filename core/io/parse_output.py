import pandas as pd
from esloss.datamodel.lossvalues import ELossCategory
from openquake.commonlib.datastore import DataStore
from openquake.risklib.scientific import LOSSTYPE


def parse_aggregated_losses(dstore: DataStore) -> pd.DataFrame:

    all_agg_keys = [d.decode().split(',')for d in dstore['agg_keys'][:]]
    oq_parameter_inputs = dstore['oqparam']
    df = dstore.read_df('risk_by_event')  # get risk_by_event

    # there is one more agg key for the 'TOTAL'
    df = df.loc[df['agg_id'] != len(all_agg_keys)]

    assert 'site_id' not in oq_parameter_inputs.aggregate_by[0]
    assert oq_parameter_inputs.calculation_mode == 'scenario_risk'

    df = df.rename(columns={'event_id': 'eventid',
                            'agg_id': 'aggregationtags',
                            'loss_id': 'losscategory',
                            'variance': 'loss_uncertainty',
                            'loss': 'loss_value'})

    df['losscategory'] = df['losscategory'].apply(
        lambda x: ELossCategory[LOSSTYPE[x].upper()])

    df['aggregationtags'] = df['aggregationtags'].apply(
        lambda x: all_agg_keys[x])
    return df
