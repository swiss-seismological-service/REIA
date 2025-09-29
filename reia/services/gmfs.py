import configparser
import csv
import json
import logging
import os
from io import StringIO
from pathlib import Path
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

from reia.utils import write_buffer_temp

LOGGER = logging.getLogger(__name__)


class GMFService:
    """Service wrapping the GMF sampling and export helpers."""

    def sample_from_csv(
        self,
        exposure_xml: Path,
        csv_files: list[str],
        imts: list[str],
        gmf_cols: list[str] | None = None,
        uncertainty_cols: list[str] | None = None,
        num_gmfs: int = 500,
        assoc_distance: float = 10,
        truncation_level: float = 2,
        seed: int = 42,
        minimum_intensities: dict = {}
    ) -> tuple[StringIO, StringIO]:
        """Sample ground motion fields (GMFs) from CSV files."""

        # create a SiteCollection
        mesh, _ = Exposure.read(
            [str(exposure_xml)], check_dupl=False).get_mesh_assets_by_site()
        full_sitecol = SiteCollection.from_points(
            mesh.lons, mesh.lats)

        # read csv files and merge into one dataframe
        dfs = [pd.read_csv(csv_file, delimiter=",") for csv_file in csv_files]

        # merge all dataframes on lat/lon
        gmfs = dfs[0]
        for df in dfs[1:]:
            gmfs = gmfs.merge(df, on=['lat', 'lon'], how='outer')

        gmfs = gmfs.dropna()

        if gmf_cols is not None:
            gmfs = gmfs.rename(columns={col: imt for col, imt in
                                        zip(gmf_cols, imts)})
        if uncertainty_cols is not None:
            gmfs = gmfs.rename(columns={col: f'{imt}_std' for col, imt in
                                        zip(uncertainty_cols, imts)})
        len_before = gmfs.shape[0]

        # filter out sites with low minimum intensity values
        for imt in imts:
            if imt in minimum_intensities:
                min_intensity = minimum_intensities[imt]
                gmfs = gmfs[gmfs[imt] / 100 >= min_intensity]

        # if gmfs are empty, check if there were any gmfs
        # passed in the first place
        if gmfs.empty:
            if len_before == 0:
                LOGGER.warning('Input to GMF sampling is empty.')
            # if all were filtered out, log a warning and return zero-valued
            else:
                LOGGER.warning('Input GMFs are lower than the '
                               'configured minimum intensities.')

            empty_gmfs = np.zeros((1, num_gmfs, 2))
            return self._write_gmf_csvs(
                empty_gmfs, full_sitecol, full_sitecol, imts)

        # calculate association distance
        # assoc_distance = self._calculate_mean_association_distance(gmfs, 3)

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
            return self._write_gmf_csvs(
                empty_gmfs, full_sitecol, full_sitecol, imts)

        # Convert CSV data to the same structured array format
        # as OpenQuake shakemap
        N = len(gmfs)

        # Create structured array with the same format as OpenQuake shakemap
        dtype = [('lon', '<f4'), ('lat', '<f4'), ('vs30', '<f4'),
                 ('val', [(imt, '<f4') for imt in imts]),
                 ('std', [(imt, '<f4') for imt in imts])]

        shakemap = np.zeros(N, dtype=dtype)
        shakemap['lon'] = gmfs['lon']
        shakemap['lat'] = gmfs['lat']
        shakemap['vs30'] = None  # Default VS30 value

        # Convert CSV data to OpenQuake shakemap format
        for imt in imts:
            shakemap['val'][imt] = gmfs[imt]  # Values in %g
            shakemap['std'][imt] = gmfs[f'{imt}_std']

        gmf_dict = {'kind': 'basic'}

        LOGGER.info(f'Sampling {num_gmfs} GMFs for {len(sitecol)} sites.')
        _, gmfs = self.to_gmfs(
            shakemap, gmf_dict, False, truncation_level, num_gmfs, seed, imts)

        LOGGER.info('Writing GMFs to CSV files.')
        return self._write_gmf_csvs(gmfs, full_sitecol, sitecol, imts)

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

        return self._write_gmf_csvs(gmfs, full_sitecol, sitecol, imts)

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
            gmf_dict: Dictionary with 'kind' key specifying method
                ('basic' or 'mmi')
            vs30: VS30 values (ignored in this implementation)
            truncation_level: Truncation level for random sampling
            num_gmfs: Number of GMF realizations to generate
            seed: Random seed for reproducible results
            imts: List of intensity measure types

        Returns:
            Tuple of (imts_list, gmfs_array)
            where gmfs_array has shape (N, E, M)
        """
        method = gmf_dict.get('kind', 'basic')
        if method not in ['basic', 'mmi']:
            raise ValueError(
                f"Unsupported method: {method}. Only "
                "'basic' and 'mmi' are supported.")

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
                    mu_vals.append(
                        np.ones(num_gmfs)
                        * np.log(
                            shakemap[j]['val'][imt]))
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
        full_sitecol,
        sitecol,
        imts: list[str],
    ) -> Tuple[StringIO, StringIO]:
        """Write the GMFs and site collection to StringIO CSV buffers."""

        sites_buf = StringIO()
        gmfs_buf = StringIO()

        # Write sites
        writer = csv.writer(sites_buf)
        writer.writerow(['site_id', 'lon', 'lat'])
        for site in full_sitecol:
            writer.writerow([
                site.id,
                round(site.location.longitude, 5),
                round(site.location.latitude, 5),
            ])

        # Write gmfs
        writer = csv.writer(gmfs_buf)
        writer.writerow(['sid', 'eid'] + [f'gmv_{imt}' for imt in imts])
        for sid, samples in zip(sitecol.sids, gmfs):
            for eid, val in enumerate(samples):
                writer.writerow([
                    sid,
                    eid,
                    round(val[0], 5),
                    round(val[1], 5),
                ])

        # rewind to the beginning so the caller can read from them
        sites_buf.seek(0)
        sites_buf.name = 'sites.csv'
        gmfs_buf.name = 'gmfs.csv'
        gmfs_buf.seek(0)

        return (gmfs_buf, sites_buf)

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


class GMFConfigurationService:
    @staticmethod
    def parse_hazard_source(config: configparser.ConfigParser,
                            exposure_xml: StringIO,
                            exposure_csv: StringIO
                            ) -> tuple[StringIO, StringIO]:
        """Parse hazard source configuration from config file.

        Args:
            config: ConfigParser object with hazard source section.

        """

        imts = [x.strip() for x in config['hazard']
                ['intensity_measure_types'].split(',')]
        truncation_level = config['hazard'].getfloat('truncation_level', None)
        random_seed = config['hazard'].getint('random_seed', None)
        asset_hazard_distance = config['hazard'].getfloat(
            'asset_hazard_distance', None)
        n_gmfs = config['hazard'].getint('number_of_ground_motion_fields', None)
        minimum_intensity = json.loads(config["hazard"]["minimum_intensity"])

        # write exposure xml and csv to temporary files
        exp_xml, tmp_dir = write_buffer_temp(exposure_xml, 'exposure.xml')
        exp_csv, tmp_dir = write_buffer_temp(
            exposure_csv, 'exposure_assets.csv', tmp_dir)

        # parse the lists of GMF and uncertainty columns
        if config['hazard_source']['source_type'] == 'custom_csv':
            gmf_cols = [x.strip() for x in
                        config['hazard_source']['gmf_cols'].split(',')]
            uncertainty_cols = [x.strip() for x in
                                config['hazard_source']
                                ['uncertainty_cols'].split(',')]
            csv_files = [x.strip() for x in
                         config['hazard_source']['files'].split(',')]

        gmfs_service = GMFService()
        return gmfs_service.sample_from_csv(
            exp_xml,
            csv_files,
            imts,
            gmf_cols,
            uncertainty_cols,
            n_gmfs,
            asset_hazard_distance,
            truncation_level,
            random_seed,
            minimum_intensity
        )
