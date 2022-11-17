import pandas as pd
from esloss.datamodel import AggregationTag, ELossCategory
from openquake.commonlib.datastore import read
from openquake.risklib.scientific import LOSSTYPE

from core.io.parse_input import parse_exposure


def get_losses(path: str):
    dstore = read(path)

    all_agg_keys = [d.decode().split(',')
                    for d in dstore['agg_keys'][:]]

    df = dstore.read_df('risk_by_event')  # get risk_by_event

    weights = dstore['weights'][:]
    events = dstore.read_df('events', 'id')[['rlz_id']]

    # risk by event contains more agg_id's than keys which
    # are used to store the total per agg value. Remove them.
    df = df.loc[df['agg_id'] != len(all_agg_keys)]

    df = df.rename(columns={'event_id': 'eventid',
                            'agg_id': 'aggregationtags',
                            'loss_id': 'losscategory',
                            'variance': 'loss_uncertainty',
                            'loss': 'loss_value'})

    df.drop('loss_uncertainty', axis=1, inplace=True)

    df['losscategory'] = df['losscategory'].apply(
        lambda x: ELossCategory[LOSSTYPE[x].upper()])

    df['aggregationtags'] = df['aggregationtags'].apply(
        lambda x: all_agg_keys[x])

    # events have an associated weight which comes from the branch weight
    events['weight'] = events['rlz_id'].apply(lambda x: weights[x])

    # number of ground motion fields * number of branches
    num_events = len(events)

    df['weight'] = df['eventid'].map(events['weight']) / num_events

    return df


def get_damages(path: str):
    dstore = read(path)

    oqparams = dstore['oqparam']
    # all_keys = list(dstore.keys())

    all_agg_keys = [d.decode().split(',')
                    for d in dstore['agg_keys'][:]]

    df = dstore.read_df('risk_by_event')  # get risk_by_event

    weights = dstore['weights'][:]

    events = dstore.read_df('events', 'id')[['rlz_id']]

    # risk by event contains more agg_id's than keys which
    # are used to store the total per agg value. Remove them.
    df = df.loc[df['agg_id'] != len(all_agg_keys)]

    df = df.rename(columns={'event_id': 'eventid',
                            'agg_id': 'aggregationtags',
                            'loss_id': 'losscategory',
                            'dmg_1': 'dg1_value',
                            'dmg_2': 'dg2_value',
                            'dmg_3': 'dg3_value',
                            'dmg_4': 'dg4_value',
                            'dmg_5': 'dg5_value', })

    df['losscategory'] = df['losscategory'].apply(
        lambda x: ELossCategory[oqparams.loss_types[x].upper()])

    df['aggregationtags'] = df['aggregationtags'].apply(
        lambda x: all_agg_keys[x])

    # events have an associated weight which comes from the branch weight
    events['weight'] = events['rlz_id'].apply(lambda x: weights[x])

    # number of ground motion fields * number of branches
    num_events = len(events)

    df['weight'] = df['eventid'].map(events['weight']) / num_events

    return df


def aggregationtags_from_files(files: list[str],
                               aggregation_types: list[str],
                               existing_tags: dict) -> list[AggregationTag]:

    asset_collection = pd.DataFrame()

    for exposure in files:
        with open(exposure, 'r') as f:
            _, assets = parse_exposure(f)
            asset_collection = pd.concat([asset_collection, assets])

    aggregation_tags = {}

    for agg_type in aggregation_types:
        all_tags = asset_collection[agg_type].unique()
        existing_tags_type = {
            t.name: t for t in existing_tags[agg_type]}
        for tag in all_tags:
            aggregation_tags[tag] = \
                existing_tags_type[tag] if tag in existing_tags_type \
                else AggregationTag(type=agg_type, name=tag)
    return aggregation_tags
