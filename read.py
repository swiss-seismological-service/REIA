# import h5py
# import pandas as pd
# import numpy as np

# f = h5py.File('/home/schmidni/oqdata/calc_18.hdf5', 'r')
# print(list(f.keys()))
# print(pd.DataFrame(np.array(f['risk_by_event'])))


# from openquake.calculators.extract import Extractor

# extractor = Extractor(calc_id=-1)

# # for k in list(f.keys()):
# #     if not k == 'avg_gmf':
# #         print(k)
# #         print(extractor.get(k))

# print(extractor.get('agg_keys').array)

# from openquake.baselib import hdf5
# from openquake.commonlib import datastore

# ds = datastore.read(-1)

# print(ds.get('agg_keys', ''))
# for key in ds:
#     print(key)

# df = ds.read_df('risk_by_event')
# print(df)
# print(df.sort_values(df.columns[0]))

# print(type(df))
# print(ds['agg_keys'])
# for key in ds['agg_keys']:
#     print(key)


# ds.close()


import h5py

f = h5py.File('/home/schmidni/oqdata/calc_34.hdf5')
# for key, value in f.attrs.items():
#     print(key)
#     print(value)
# print(f.attrs.values())
print(f['agg_keys'])
for item in f['agg_keys']:
    print(item)
