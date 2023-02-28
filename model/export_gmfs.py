
from openquake.commonlib.datastore import read

# pd.set_option('display.max_rows', None)
dstore = read('test_model/calc_27257.hdf5')

oq_parameter_inputs = dstore['oqparam']
all_keys = list(dstore.keys())

all_agg_keys = [d.decode().split(',')
                for d in dstore['agg_keys'][:]]

total_values_per_agg_key = dstore.read_df('agg_values')

site_collection = dstore.read_df('sitecol')[['sids', 'lon', 'lat']]
gmf_data = dstore.read_df('gmf_data')

site_collection.rename(columns={'sids': 'site_id'}, inplace=True)

with open('gmfs.csv', 'w') as f:
    gmf_data.to_csv(f, index=False)

with open('sites.csv', 'w') as f:
    site_collection.to_csv(f, index=False)
