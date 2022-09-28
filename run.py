
import pandas as pd
from esloss.datamodel.asset import AggregationTag
from esloss.datamodel.lossvalues import AggregatedLoss, ELossCategory
from openquake.commonlib.datastore import read
from openquake.risklib.scientific import LOSSTYPE
from sqlalchemy import select

from core.db import session

pd.set_option('display.max_rows', None)
dstore = read(65)  # site_id
dstore = read(60)  # Canton


oq_parameter_inputs = dstore['oqparam']
all_keys = list(dstore.keys())

all_agg_keys = [d.decode().split(',')
                for d in dstore['agg_keys'][:]]

total_values_per_agg_key = dstore.read_df('agg_values')

df = dstore.read_df('risk_by_event')  # get risk_by_event
# df = dstore.read_df('risk_by_event', 'event_id')  # indexed with event_id

# either or:
# all_agg_keys.append(['Total']) #
df = df.loc[df['agg_id'] != len(all_agg_keys)]

# how to query
# print(df.loc[(df['loss_id'] == 0) & (df['event_id'] == 0.)])
# print(df.loc[(df['loss_id'] == 0) & (df['event_id'] == 0.)]['loss'].mean())
# print(df.loc[(df['loss_id'] == 0) & (df['event_id'] == 0.)].shape[0])

assert('site_id' not in oq_parameter_inputs.aggregate_by[0])
assert(oq_parameter_inputs.calculation_mode == 'scenario_risk')

agg_types = oq_parameter_inputs.aggregate_by[0]

df = df.rename(columns={'event_id': 'eventid',
                        'agg_id': 'aggregationtags',
                        'loss_id': 'losscategory',
                        'variance': 'loss_uncertainty',
                        'loss': 'loss_value'})

df['losscategory'] = df['losscategory'].apply(
    lambda x: ELossCategory[LOSSTYPE[x].upper()])

df['aggregationtags'] = df['aggregationtags'].apply(
    lambda x: all_agg_keys[x])

aggregations = {}
for type in agg_types:
    stmt = select(AggregationTag).where(
        AggregationTag.type == type,
        AggregationTag._assetcollection_oid == 1)
    type_tags = session.execute(stmt).scalars().all()
    aggregations.update({tag.name: tag for tag in type_tags})

df['aggregationtags'] = df['aggregationtags'].apply(
    lambda x: [aggregations[y] for y in x])

loss_objects = list(map(lambda x: AggregatedLoss(**x, _losscalculation_oid=1),
                        df.to_dict('records')))

print(df)
