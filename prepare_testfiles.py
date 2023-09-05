import pandas as pd

exp = pd.read_csv('reia/tests/data/ria_test/exposure.csv')

exp.drop(['Occupancy', 'typ', 'gridcell2',
         'CantonGemeindePC'], axis=1, inplace=True)
exp = exp[((exp['CantonGemeinde'] == 'GR3911') | (
    exp['CantonGemeinde'] == 'SG3296')) & (exp['taxonomy'] == 'M5_L_A4')]

# print(exp)
exp.to_csv('reia/tests/data/ria_test/exposure_test.csv', index=False)

gmfs = pd.read_csv('reia/tests/data/ria_test/gmfs.csv')


sites = pd.read_csv('reia/tests/data/ria_test/sites.csv')
sites_sg = sites[(sites['lon'] > 9.38) & (sites['lon'] < 9.48)
                 & (sites['lat'] > 47.0) & (sites['lat'] < 47.1)]

site_ids = list(sites_sg['site_id'])

sites_gr = sites[(sites['lon'] > 9.5) & (sites['lon'] < 9.55)
                 & (sites['lat'] > 46.76) & (sites['lat'] < 46.8)]
site_ids.extend(list(sites_gr['site_id']))


gmfs = gmfs[gmfs['sid'].isin(site_ids)]
gmfs.to_csv('reia/tests/data/ria_test/gmfs_test.csv', index=False)
