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
    """
    Sample ground motion fields (GMFs) from CSV files.

    Parameters
    ----------
    exposure_xml : str
        Path to the exposure XML file.
    psa03_csv : str
        Path to the CSV file containing the PSA03 values.
    psa06_csv : str
        Path to the CSV file containing the PSA06 values.

    Returns
    -------
    None
    """

    # create a SiteCollection
    mesh, _ = Exposure.read(
        exposure_xml, check_dupl=False).get_mesh_assets_by_site()
    full_sitecol = SiteCollection.from_points(
        mesh.lons, mesh.lats)

    # read csv files and merge into one dataframe
    psa03 = pd.read_csv(psa03_csv, delimiter=",")
    psa06 = pd.read_csv(psa06_csv, delimiter=",")
    gmfs = psa03.merge(psa06, on=['lat', 'lon'], how='inner').dropna()

    # unit conversion
    gmfs['psa06'] = gmfs['psa06'] / 9.80665
    gmfs['psa03'] = gmfs['psa03'] / 9.80665

    # filter out sites with low psa values
    thresholds = [(0.05, 0.1), (0.005, 0.01),
                  (0.0005, 0.001), (0.0001, 0.0005)]

    # filter adaptively so that there are some sites left
    for ts in thresholds:
        df = gmfs[(gmfs['psa06'] > ts[0]) & (gmfs['psa03'] > ts[1])]
        if not df.empty:
            gmfs = df
            print(ts)
            break

    # convert back to array
    gmfs = gmfs.to_records(index=False)

    # associate gmfs back to sitecollection assoc_dist must be relative to
    # resolution of gmf input to make sure that they get associated to the
    # correct sites
    # TODO: make assoc_dist dynamic
    sitecol, gmfs, discarded = geo.utils.assoc(
        gmfs, full_sitecol, 1.32, 'filter')

    imts = ['psa03', 'psa06']
    stds = ['lnpsa03_uncertainty', 'lnpsa06_uncertainty']

    # build up gmfs matrix for sampling
    M = len(imts)       # Number of imts
    N = len(gmfs)       # number of sites

    num_gmfs = 500

    # SAMPLING
    # generate standard normal random variates of shape (M*N, E)
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
