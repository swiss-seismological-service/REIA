import pandas as pd
from esloss.datamodel import ELossCategory
from openquake.commonlib.datastore import read

from core.io.parse_input import parse_exposure


def get_risk_from_dstore(path: str, column_selectors: dict):
    dstore = read(path)

    all_agg_keys = [d.decode().split(',')
                    for d in dstore['agg_keys'][:]]

    df = dstore.read_df('risk_by_event')  # get risk_by_event

    weights = dstore['weights'][:]
    events = dstore.read_df('events', 'id')[['rlz_id']]

    # risk by event contains more agg_id's than keys which
    # are used to store the total per agg value. Remove them.
    df = df.loc[df['agg_id'] != len(all_agg_keys)]

    df = df.rename(columns=column_selectors)[column_selectors.values()]

    lti = {v: k for k, v in dstore['oqparam'].lti.items()}
    df['losscategory'] = df['losscategory'].apply(
        lambda x: ELossCategory[lti[x].upper()])

    df['aggregationtags'] = df['aggregationtags'].apply(
        lambda x: all_agg_keys[x])

    # events have an associated weight which comes from the branch weight
    events['weight'] = events['rlz_id'].apply(lambda x: weights[x])

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
