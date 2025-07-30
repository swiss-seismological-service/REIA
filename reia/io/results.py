import os
from operator import attrgetter

import pandas as pd
from openquake.commonlib.datastore import DataStore
from openquake.risklib.scientific import LOSSTYPE

from reia.io import RISK_COLUMNS_MAPPING
from reia.schemas.calculation_schemas import CalculationBranch
from reia.schemas.enums import ELossCategory, ERiskType


def extract_risk_from_datastore(dstore: DataStore,
                                risk_type: ERiskType) -> pd.DataFrame:
    """Extract risk data from OpenQuake datastore.

    Args:
        dstore: OpenQuake datastore containing calculation results
        risk_type: Type of risk calculation (LOSS or DAMAGE)

    Returns:
        DataFrame with processed risk values
    """
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

    df['weight'] = df['eventid'].map(events['weight']) / \
        dstore['oqparam'].number_of_ground_motion_fields

    if risk_type == ERiskType.DAMAGE:
        df = df[(df['dg1_value'] > 0)
                | (df['dg2_value'] > 0)
                | (df['dg3_value'] > 0)
                | (df['dg4_value'] > 0)
                | (df['dg5_value'] > 0)]

    return df


def prepare_risk_data_for_storage(
        risk_values: pd.DataFrame,
        calculationbranch: CalculationBranch,
        risk_type: ERiskType,
        aggregation_tag_by_name: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Prepare risk data for database storage.

    Args:
        risk_values: Raw risk values DataFrame from OpenQuake extraction
        calculationbranch: The calculation branch object
        risk_type: Type of risk calculation (LOSS or DAMAGE)
        aggregation_tag_by_name: Dictionary mapping tag names to tag objects

    Returns:
        Tuple of (processed_risk_values, aggregation_mappings) ready for
        database insertion
    """
    # Create a copy to avoid modifying the original
    risk_values = risk_values.copy()
    # Add calculation metadata
    risk_values['weight'] *= calculationbranch.weight
    risk_values['_calculation_oid'] = calculationbranch.calculation_oid
    risk_values['_calculationbranch_oid'] = calculationbranch.oid
    risk_values['_type'] = risk_type.name
    risk_values['losscategory'] = risk_values['losscategory'].map(
        attrgetter('name'))
    risk_values['_oid'] = pd.RangeIndex(start=1, stop=len(risk_values) + 1)

    # Build many-to-many reference table
    df_agg_val = pd.DataFrame({
        'riskvalue': risk_values['_oid'],
        'aggregationtag': risk_values.pop('aggregationtags'),
        '_calculation_oid': risk_values['_calculation_oid'],
        'losscategory': risk_values['losscategory']
    })

    # Explode aggregation tags (list -> rows)
    df_agg_val = df_agg_val.explode('aggregationtag', ignore_index=True)

    # Map to tag object
    tag_objs = df_agg_val['aggregationtag'].map(aggregation_tag_by_name)
    df_agg_val['aggregationtype'] = tag_objs.map(attrgetter('type'))
    df_agg_val['aggregationtag'] = tag_objs.map(attrgetter('oid'))

    return risk_values, df_agg_val
