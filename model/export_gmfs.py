
from openquake.commonlib.datastore import read

# pd.set_option('display.max_rows', None)
dstore = read('calc_11.hdf5')

oq_parameter_inputs = dstore['oqparam']
all_keys = list(dstore.keys())

all_agg_keys = [d.decode().split(',')
                for d in dstore['agg_keys'][:]]

total_values_per_agg_key = dstore.read_df('agg_values')

site_collection = dstore.read_df('sitecol', 'sids')
gmf_data = dstore.read_df('gmf_data', 'eid')
gmf_data.rename(columns={'gmv_0': 'gmv_MMI'}, inplace=True)

with open('gmfs.csv', 'w') as f:
    gmf_data.to_csv(f)

with open('sites.csv', 'w') as f:
    site_collection.to_csv(f)
