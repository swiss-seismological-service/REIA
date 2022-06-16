from openquake.calculators.extract import WebExtractor
from openquake.commonlib.datastore import read
import pandas as pd
from esloss.datamodel.lossvalues import ELossCategory
from esloss.datamodel.vulnerability import VulnerabilityModel

# print(ELossCategory['structural'.upper()])
# pd.set_option('display.max_rows', None)
dstore = read(-1)
# print(dstore['oqparam'])
# print(list(dstore.keys()))
# print([d.decode().split(',') for d in dstore['agg_keys'][:]])
# print(dstore['oqparam'].loss_types)
# print(dstore['oqparam'].aggregate_by[0])
# df = dstore.read_df('risk_by_event')
# df = dstore.read_df('risk_by_event', 'event_id')
# print(df)
# print(df.loc[(df['loss_id'] == 0) & (df['event_id'] == 0.)])
# print(df.loc[(df['loss_id'] == 0) & (df['event_id'] == 0.)]['loss'].mean())
# print(df.loc[(df['loss_id'] == 0) & (df['event_id'] == 0.)].shape[0])
# print(dstore.read_df('agg_values'))
# print(dstore['risk_by_event']['agg_id'])

# extractor = WebExtractor(47, server='http://localhost:8800')
# obj = extractor.get('risk_by_event')
# print(obj.array)
# extractor.close()

assert(dstore['oqparam'].calculation_mode == 'scenario_risk')

agg_keys = [d.decode().split(',') for d in dstore['agg_keys'][:]]
loss_types = dstore['oqparam'].loss_types

print(dstore['oqparam'].aggregate_by[0])

df = dstore.read_df('risk_by_event')
df = df.rename(columns={'event_id': 'eventid',
                        'agg_id': 'aggregationtags',
                        'loss_id': 'losscategory',
                        'variance': 'loss_uncertainty',
                        'loss': 'loss_value'})

df['losscategory'] = df['losscategory'].apply(
    lambda x: ELossCategory[loss_types[x].upper()])
df['aggregationtags'] = df['aggregationtags'].apply(
    lambda x: agg_keys[x - 1])
print(df)
