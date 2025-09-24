import csv
import errno
import logging
import os
import sys
from typing import Tuple

import geopandas as gpd
import numpy as np
import pandas as pd
from geopy import distance
from openquake.commonlib.datastore import read
from openquake.hazardlib import geo
from openquake.hazardlib.shakemap.maps import get_sitecol_shakemap
from openquake.hazardlib.site import SiteCollection
from openquake.risklib.asset import Exposure
from scipy.stats import truncnorm

LOGGER = logging.getLogger(__name__)


class GMFService:
    """Service wrapping the GMF sampling and export helpers."""

    def sample_from_csv(
        self,
        exposure_xml: list[str],
        csv_files: list[str],
        num_gmfs: int = 500,
    ) -> None:
        """Sample ground motion fields (GMFs) from CSV files."""

        # create a SiteCollection
        mesh, _ = Exposure.read(
            exposure_xml, check_dupl=False).get_mesh_assets_by_site()
        full_sitecol = SiteCollection.from_points(
            mesh.lons, mesh.lats)

        # read csv files and merge into one dataframe
        dfs = [pd.read_csv(csv_file, delimiter=",") for csv_file in csv_files]

        # merge all dataframes on lat/lon
        gmfs = dfs[0]
        for df in dfs[1:]:
            gmfs = gmfs.merge(df, on=['lat', 'lon'], how='outer')

        # fill missing values with 0
        gmfs = gmfs.fillna(0)

        # filter array, correct unit (%g to g) - keep values in original scale for filtering
        filter_psa06 = gmfs['psa06_%g'] / 100
        filter_psa03 = gmfs['psa03_%g'] / 100

        # Keep original values in %g - no log transformation here

        # filter out sites with low psa values (in g units after /100 conversion)
        # thresholds were always in normal space since they compare against filter_psa* variables
        thresholds = [(0.05, 0.1), (0.005, 0.01), (0.0005, 0.001)]
        len_before = gmfs.shape[0]

        # filter adaptively so that there are some sites left
        for ts in thresholds:
            df = gmfs[(filter_psa06 > ts[0]) & (filter_psa03 > ts[1])]
            if not df.empty:
                break
        gmfs = df

        # if gmfs are empty, check if there were any gmfs passed in the first
        # place
        if gmfs.empty:
            if len_before == 0:
                LOGGER.error('Input to GMF sampling is empty.')
                sys.exit(errno.EINVAL)

            # if all were filtered out, log a warning and return zero-valued
            # gmfs
            LOGGER.warning('Input GMFs are too small.')
            empty_gmfs = np.zeros((1, num_gmfs, 2))
            self._write_gmf_csvs(empty_gmfs, full_sitecol, full_sitecol)
            return

        # calculate association distance
        assoc_distance = self._calculate_mean_association_distance(gmfs, 3)

        # convert back to numpy arrays
        gmfs = gmfs.to_records(index=False)

        # associate gmfs back to sitecollection assoc_dist must be relative to
        # resolution of gmf input to make sure that they get associated to the
        # correct sites
        try:
            sitecol, gmfs, discarded = geo.utils.assoc(
                gmfs, full_sitecol, assoc_distance, 'filter')
        except geo.utils.SiteAssociationError:
            # if no sites could be associated, check how many sites are in
            # Switzerland
            ch, num = self._isin_switzerland(gmfs)
            LOGGER.warning(f'Error: {num} GMVs are in Switzerland, '
                           f'{gmfs.shape[0]-num} are outside of Switzerland.')
            # and return zero-valued gmfs
            empty_gmfs = np.zeros((1, num_gmfs, 2))
            self._write_gmf_csvs(empty_gmfs, full_sitecol, full_sitecol)
            return

        # Convert CSV data to the same structured array format as OpenQuake shakemap
        N = len(gmfs)
        imts = ['psa03_%g', 'psa06_%g']

        # Create structured array with the same format as OpenQuake shakemap
        dtype = [('lon', '<f4'), ('lat', '<f4'), ('vs30', '<f4'),
                 ('val', [(imt, '<f4') for imt in imts]),
                 ('std', [(imt, '<f4') for imt in imts])]

        shakemap = np.zeros(N, dtype=dtype)
        shakemap['lon'] = gmfs['lon']
        shakemap['lat'] = gmfs['lat']
        shakemap['vs30'] = 600.0  # Default VS30 value

        # Convert CSV data to OpenQuake shakemap format - values are in original %g scale
        shakemap['val']['psa03_%g'] = gmfs['psa03_%g']  # Values in %g
        shakemap['val']['psa06_%g'] = gmfs['psa06_%g']  # Values in %g
        shakemap['std']['psa03_%g'] = gmfs['lnpsa03_uncertainty']  # Std in log space
        shakemap['std']['psa06_%g'] = gmfs['lnpsa06_uncertainty']  # Std in log space

        gmf_dict = {'kind': 'basic'}

        _, gmfs = self.to_gmfs(
            shakemap, gmf_dict, False, 2, num_gmfs, 41, imts)

        self._write_gmf_csvs(gmfs, full_sitecol, sitecol)

    def sample_from_shakemap(
        self,
        exposure_xml: list[str],
        grid_xml: str,
        uncertainty_xml: str,
        imts: list[str],
    ) -> None:
        """Sample ground motion fields (GMFs) from shakemap files."""

        mesh, assets_by_site = Exposure.read(
            exposure_xml, check_dupl=False).get_mesh_assets_by_site()
        full_sitecol = SiteCollection.from_points(
            mesh.lons, mesh.lats)

        uridict = {"kind": 'usgs_xml',
                   "grid_url": grid_xml,
                   "uncertainty_url": uncertainty_xml}

        sitecol, shkmp, discarded = get_sitecol_shakemap(
            uridict, imts, full_sitecol, mode='filter')

        gmf_dict = {'kind': 'basic'}

        _, gmfs = self.to_gmfs(
            shkmp, gmf_dict, False, 2, 100, 42, imts)

        self._write_gmf_csvs(
            gmfs, full_sitecol, sitecol, [f'gmv_{imt}' for imt in imts])

    def to_gmfs(
        self,
        shakemap,
        gmf_dict,
        vs30,
        truncation_level,
        num_gmfs,
        seed,
        imts=None
    ) -> Tuple[list, np.ndarray]:
        """Generate ground motion fields from shakemap data.

        Compatible with OpenQuake's to_gmfs signature but only supports
        'basic' and 'mmi' calculation methods.

        Args:
            shakemap: Dictionary containing site data and ground motion values
            gmf_dict: Dictionary with 'kind' key specifying method ('basic' or 'mmi')
            vs30: VS30 values (ignored in this implementation)
            truncation_level: Truncation level for random sampling
            num_gmfs: Number of GMF realizations to generate
            seed: Random seed for reproducible results
            imts: List of intensity measure types

        Returns:
            Tuple of (imts_list, gmfs_array) where gmfs_array has shape (N, E, M)
        """
        method = gmf_dict.get('kind', 'basic')
        if method not in ['basic', 'mmi']:
            raise ValueError(f"Unsupported method: {method}. Only 'basic' and 'mmi' are supported.")

        if imts is None:
            imts = list(shakemap.imts)

        M = len(imts)  # Number of IMTs
        N = len(shakemap)  # Number of sites

        # Generate truncated normal random variables
        Z = truncnorm.rvs(-truncation_level, truncation_level,
                         loc=0, scale=1,
                         size=(M * N, num_gmfs),
                         random_state=seed)

        if method == 'basic':
            # Basic method: log-normal distribution with %g to g conversion
            mu_vals = []
            sig_vals = []

            for imt in imts:
                for j in range(N):
                    # Take log of values and apply standard processing
                    mu_vals.append(np.ones(num_gmfs) * np.log(shakemap[j]['val'][imt]))
                    sig_vals.append(shakemap[j]['std'][imt])

            mu = np.array(mu_vals)
            sig = np.array(sig_vals)

            # Apply variance correction for log-normal distribution
            mu = mu - (sig.reshape(-1, 1) ** 2 / 2)

            # Generate samples and convert from %g to g
            gmfs = np.exp((Z.T * sig).T + mu) / 100

        elif method == 'mmi':
            # MMI method: simple normal distribution
            mu_vals = []
            sig_vals = []

            for imt in imts:
                for j in range(N):
                    # Access the IMT-specific values from the structured array
                    mu_vals.append(np.ones(num_gmfs) * shakemap[j]['val'][imt])
                    sig_vals.append(shakemap[j]['std'][imt])

            mu = np.array(mu_vals)
            sig = np.array(sig_vals)

            gmfs = (Z.T * sig).T + mu

        # Reshape to (N, E, M) format
        gmfs = gmfs.reshape((M, N, num_gmfs)).transpose(1, 2, 0)

        return imts, gmfs

    def export_from_datastore(
        self,
        dstore: str,
    ) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """Read the GMFs and site collection from the datastore."""

        store = read(dstore)

        site_collection = store.read_df('sitecol')[['sids', 'lon', 'lat']]
        gmf_data = store.read_df('gmf_data')

        # rename gmv_* columns based on IMT metadata stored in the datastore
        gmf_attrs = store.hdf5['gmf_data'].attrs
        imts_attr = gmf_attrs.get('imts', '')
        if isinstance(imts_attr, bytes):
            imts_attr = imts_attr.decode()
        imts = [imt.strip() for imt in imts_attr.split() if imt.strip()]

        rename_map = {}
        for idx, imt in enumerate(imts):
            col = f'gmv_{idx}'
            renamed = f'gmv_{imt}'
            if col in gmf_data.columns and renamed not in gmf_data.columns:
                rename_map[col] = renamed

        if rename_map:
            gmf_data = gmf_data.rename(columns=rename_map)

        site_collection.rename(columns={'sids': 'site_id'}, inplace=True)

        return (gmf_data, site_collection)

    def _write_gmf_csvs(
        self,
        gmfs: np.ndarray,
        full_sitecol: SiteCollection,
        sitecol: SiteCollection,
        sa_levels: list[str] = ['gmv_SA(0.3)', 'gmv_SA(0.6)'],
    ) -> None:
        """Write the GMFs and site collection to CSV files."""

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
            writer.writerow(['sid', 'eid'] + sa_levels)

            for sid, samples in zip(sitecol.sids, gmfs):
                for eid, val in enumerate(samples):
                    writer.writerow(
                        [sid, eid, round(val[0], 5), round(val[1], 5)])

    def _calculate_mean_association_distance(
        self,
        data: pd.DataFrame,
        factor: float,
    ) -> float:
        """
        Calculate the mean resolution of coordinate
        grid and association distance.
        """

        longitudes = pd.Series(data['lon'].sort_values().unique())
        latitudes = pd.Series(data['lat'].sort_values().unique())

        # in this case, we can probably not calculate
        # a meaningful association distance
        if longitudes.shape[0] < 50 or latitudes.shape[0] < 50:
            return 2

        # get resolutions of lon grid
        distances_lon = \
            np.array([distance.distance((0, lon1), (0, lon2)).kilometers for
                      lon1, lon2 in zip(longitudes[:-1],
                                        longitudes.shift(-1)[:-1])])

        # get resolutions of lat grid
        distances_lat = \
            np.array([distance.distance((lat1, 0), (lat2, 0)).kilometers for
                      lat1, lat2 in zip(latitudes[:-1],
                                        latitudes.shift(-1)[:-1])])

        # calculate mode of lon resolution
        lon_bins = int((max(distances_lon) - min(distances_lon)) * 100) + 1
        counts_lon, bins_lon = np.histogram(distances_lon, bins=lon_bins)
        index_lon = np.unravel_index(np.argmax(counts_lon), counts_lon.shape)[0]
        mean_lon = bins_lon[index_lon:index_lon + 1].mean()

        # calculate mode of lat resolution
        lat_bins = int((max(distances_lat) - min(distances_lat)) * 100) + 1
        counts_lat, bins_lat = np.histogram(distances_lat, bins=lat_bins)
        index_lat = np.unravel_index(np.argmax(counts_lat), counts_lat.shape)[0]
        mean_lat = bins_lat[index_lat:index_lat + 1].mean()

        # calculate half of the diagonal of one grid rectangle and multiply by f
        dist = np.sqrt(mean_lon**2 + mean_lat**2) / 2 * factor

        return dist

    def _isin_switzerland(self, data: pd.DataFrame):
        """Check if the data points lie inside of Switzerland."""
        data_poly = gpd.read_file(
            os.path.join(os.path.dirname(os.path.abspath(__file__)),
                         '../data/geometries/CH.json'))
        gdf = gpd.GeoDataFrame(
            data, geometry=gpd.points_from_xy(
                data.lon, data.lat), crs='EPSG:4326')

        joined_gdf = gpd.sjoin(gdf, data_poly, predicate='within')
        return joined_gdf.empty, joined_gdf.shape[0]
