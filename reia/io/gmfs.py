import csv

import numpy as np
import pandas as pd
from openquake.hazardlib import geo
from openquake.hazardlib.shakemap.gmfs import to_gmfs
from openquake.hazardlib.shakemap.maps import get_sitecol_shakemap
from openquake.hazardlib.site import SiteCollection
from openquake.risklib.asset import Exposure
from scipy.stats import truncnorm


def sample_gmfs_from_csv(exposure_xml: str,
                         psa03_csv: str,
                         psa06_csv: str):

    mesh, assets_by_site = Exposure.read(
        exposure_xml, check_dupl=False).get_mesh_assets_by_site()
    full_sitecol = SiteCollection.from_points(
        mesh.lons, mesh.lats)

    psa03 = pd.read_csv(psa03_csv, delimiter=",")
    psa06 = pd.read_csv(psa06_csv, delimiter=",")
    gmfs = psa03.merge(psa06, on=['lat', 'lon'], how='inner').dropna()

    gmfs['psa06'] = gmfs['psa06'] / 9.80665
    gmfs['psa03'] = gmfs['psa03'] / 9.80665
    # gmfs = gmfs[(gmfs['psa06'] > 0.05) & (gmfs['psa03'] > 0.1)]
    gmfs = gmfs[(gmfs['psa06'] > 0.0001) & (gmfs['psa03'] > 0.0005)]

    gmfs = gmfs.to_records(index=False)

    sitecol, gmfs, discarded = geo.utils.assoc(
        gmfs, full_sitecol, 1.32, 'filter')

    imts = ['psa03', 'psa06']
    stds = ['lnpsa03_uncertainty', 'lnpsa06_uncertainty']

    # assign iterators
    M = len(imts)       # Number of imts
    N = len(gmfs)       # number of sites

    num_gmfs = 500

    # generate standard normal random variables of shape (M*N, E)
    Z = truncnorm.rvs(-2, 2, loc=0, scale=1,
                      size=(M * N, num_gmfs), random_state=41)

    # build array of mean values of shape (M*N, E)
    mu = np.array([np.ones(num_gmfs) * gmfs[str(imt)][j]
                   for imt in imts for j in range(N)])

    sig = np.array([gmfs[std] for std in stds]).flatten()

    gmfs = np.exp((Z.T * sig).T + np.log(mu))

    gmfs = gmfs.reshape((M, N, num_gmfs)).transpose(1, 2, 0)

    with open('sites_gen.csv', 'w') as f:
        writer = csv.writer(f)

        # write the header
        writer.writerow(['site_id', 'lon', 'lat'])

        for site in full_sitecol:
            # write the data
            writer.writerow([site.id,
                            round(site.location.longitude, 5),
                            round(site.location.latitude, 5)])

    with open('gmfs_gen.csv', 'w') as f:
        writer = csv.writer(f)

        # write the header
        writer.writerow(['sid', 'eid', 'gmv_SA(0.3)', 'gmv_SA(0.6)'])

        for sid, samples in zip(sitecol.sids, gmfs):
            for eid, val in enumerate(samples):
                writer.writerow([sid, eid, round(val[0], 5), round(val[1], 5)])


def sample_gmfs_from_shakemap(exposure_xml, grid_xml, uncertainty_xml):
    mesh, assets_by_site = Exposure.read(
        exposure_xml, check_dupl=False).get_mesh_assets_by_site()
    full_sitecol = SiteCollection.from_points(
        mesh.lons, mesh.lats)

    uridict = {"kind": 'usgs_xml',
               "grid_url": grid_xml,
               "uncertainty_url": uncertainty_xml}

    sitecol, shkmp, discarded = get_sitecol_shakemap(
        uridict, ['SA(0.3)', 'SA(1.0)'], full_sitecol, mode='filter')

    gmf_dict = {'kind': 'basic'}

    _, gmfs = to_gmfs(
        shkmp, gmf_dict, False, 2, 100, 42, [
            'SA(0.3)', 'SA(1.0)'])

    with open('sites_gen.csv', 'w') as f:
        writer = csv.writer(f)

        # write the header
        writer.writerow(['site_id', 'lon', 'lat'])

        for site in full_sitecol:
            # write the data
            if site.id == 18084:
                print(site)
            writer.writerow([site.id,
                            round(site.location.longitude, 5),
                            round(site.location.latitude, 5)])

    with open('gmfs_gen.csv', 'w') as f:
        writer = csv.writer(f)

        # write the header
        writer.writerow(['eid', 'sid', 'gmv_SA(0.3)', 'gmv_SA(1.0)'])

        for sid, samples in zip(sitecol.sids, gmfs):
            for eid, val in enumerate(samples):
                writer.writerow([eid, sid, round(val[0], 5), round(val[1], 5)])
