import os
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import TextIO

import geopandas as gpd
import pandas as pd
import shapely
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon

from reia.io import ASSETS_COLS_MAPPING
from reia.schemas.exposure_schema import ExposureModel
from reia.schemas.fragility_schemas import FragilityModel
from reia.schemas.vulnerability_schemas import VulnerabilityModel
from reia.utils import clean_array


def parse_exposure_assets(file: TextIO, tagnames: list[str]) -> pd.DataFrame:
    """Reads an exposure file with assets into a dataframe.

    Args:
        file: csv file object with headers (Input OpenQuake):
              id,lon,lat,taxonomy,number,structural,contents,day(
              CantonGemeinde,CantonGemeindePC, ...)
        tagnames: List of tag names to include.

    Returns:
        df with columns for datamodel.Assets object + lat and lon.
    """

    df = pd.read_csv(file, index_col='id')

    lonlat = {'lon': 'longitude',
              'lat': 'latitude'}

    df = df.rename(
        columns={
            k: v for k,
            v in {
                **ASSETS_COLS_MAPPING,
                **lonlat}.items() if k in df and v not in df})

    valid_cols = list(ASSETS_COLS_MAPPING.values()) + \
        tagnames + list(lonlat.values())

    df.drop(columns=df.columns.difference(valid_cols), inplace=True)

    return df


def parse_exposure_metadata(file: TextIO
                            ) -> tuple[ExposureModel, str]:
    tree = ET.iterparse(file)

    # strip namespace for easier querying
    for _, el in tree:
        _, _, el.tag = el.tag.rpartition('}')

    root = tree.root
    model = {'costtypes': []}

    # exposureModel attributes
    for child in root:
        model['publicid'] = child.attrib['id']
        model['category'] = child.attrib['category']
        model['taxonomy_classificationsource_resourceid'] = \
            child.attrib['taxonomySource']
    model['description'] = root.find('exposureModel/description').text

    # occupancy periods
    occupancyperiods = root.find(
        'exposureModel/occupancyPeriods').text.split()
    model['dayoccupancy'] = 'day' in occupancyperiods
    model['nightoccupancy'] = 'night' in occupancyperiods
    model['transitoccupancy'] = 'transit' in occupancyperiods

    # iterate cost types
    for test in root.findall('exposureModel/conversions/costTypes/costType'):
        model['costtypes'].append(test.attrib)

    model['aggregationtypes'] = root.find(
        'exposureModel/tagNames').text.split(' ')

    assets_path = root.find('exposureModel/assets').text
    assets_path = os.path.join(os.path.dirname(file.name), assets_path)

    model = ExposureModel.model_validate(model)
    return model, assets_path


def parse_fragility(file: TextIO) -> FragilityModel:
    model = {}
    model['fragilityfunctions'] = []

    tree = ET.iterparse(file)

    for _, el in tree:
        _, _, el.tag = el.tag.rpartition('}')

    root = tree.root

    for child in root:
        model['assetcategory'] = child.attrib['assetCategory']
        model['_type'] = child.attrib['lossCategory']
        model['publicid'] = child.attrib['id']

    model['description'] = root.find('fragilityModel/description').text
    model['limitstates'] = root.find(
        'fragilityModel/limitStates').text.strip().split(' ')

    # read values for VulnerabilityFunctions
    for vF in root.findall('fragilityModel/fragilityFunction'):
        fun = {}
        fun['taxonomy_concept'] = vF.attrib['id']
        fun['format'] = vF.attrib['format']
        fun['shape'] = vF.attrib.get('shape', None)

        fun['intensitymeasuretype'] = vF.find('imls').attrib['imt']
        fun['nodamagelimit'] = vF.find(
            'imls').attrib.get('noDamageLimit', None)
        fun['minintensitymeasurelevel'] = vF.find(
            'imls').attrib.get('minIML', None)
        fun['maxintensitymeasurelevel'] = vF.find(
            'imls').attrib.get('maxIML', None)
        fun['intensitymeasurelevels'] = clean_array(
            vF.find('imls').text).split(' ')

        fun['limitstates'] = []

        for poe in vF.findall('poes'):
            limit_state = {}
            limit_state['name'] = poe.attrib['ls']

            limit_state['poes'] = clean_array(poe.text).split()
            fun['limitstates'].append(limit_state)

        for params in vF.findall('params'):
            limit_state = {}
            limit_state['name'] = params.attrib['ls']
            limit_state['mean'] = params.attrib['mean']
            limit_state['stddev'] = params.attrib['stddev']

        model['fragilityfunctions'].append(fun)

    model = FragilityModel.model_validate(model)

    return model


def parse_vulnerability(file: TextIO) -> VulnerabilityModel:
    model = {}
    model['vulnerabilityfunctions'] = []

    tree = ET.iterparse(file)

    # strip namespace for easier querying
    for _, el in tree:
        _, _, el.tag = el.tag.rpartition('}')

    root = tree.root

    # read values for VulnerabilityModel
    for child in root:
        model['assetcategory'] = child.attrib['assetCategory']
        model['_type'] = child.attrib['lossCategory']
        model['publicid'] = child.attrib['id']
    model['description'] = root.find(
        'vulnerabilityModel/description').text.strip()

    # read values for VulnerabilityFunctions
    for vF in root.findall('vulnerabilityModel/vulnerabilityFunction'):
        fun = {}
        fun['taxonomy_concept'] = vF.attrib['id']
        fun['distribution'] = vF.attrib['dist']
        fun['intensitymeasuretype'] = vF.find('imls').attrib['imt']

        imls = clean_array(vF.find('imls').text).split(' ')
        meanLRs = clean_array(vF.find('meanLRs').text).split(' ')
        covLRs = clean_array(vF.find('covLRs').text).split(' ')

        fun['lossratios'] = []
        for i, m, c in zip(imls, meanLRs, covLRs):
            fun['lossratios'].append({'intensitymeasurelevel': i,
                                      'mean': m,
                                      'coefficientofvariation': c})

        model['vulnerabilityfunctions'].append(fun)

    model = VulnerabilityModel.model_validate(model)

    return model


def parse_shapefile_geometries(
        filename: Path,
        tag_column_name: str,
        aggregationtype: str) -> pd.DataFrame:
    """Parse a shapefile and prepare geometries for database insertion.

    Args:
        filename: Path to the shapefile.
        tag_column_name: Name of the aggregation tag column.
        aggregationtype: Type of the aggregation.

    Returns:
        DataFrame prepared for AggregationGeometryRepository.insert_many().
    """
    gdf = pd.DataFrame(gpd.read_file(filename))

    gdf['geometry'] = gdf['geometry'].apply(
        lambda x: MultiPolygon([x]) if isinstance(x, Polygon) else x)
    gdf['geometry'] = gdf['geometry'].apply(lambda x: shapely.force_2d(x).wkt)

    gdf = gdf[[tag_column_name, 'geometry', 'name']]
    gdf = gdf.rename(columns={tag_column_name: 'aggregationtag'})
    gdf['_aggregationtype'] = aggregationtype

    return gdf


def _extract_sites(assets: pd.DataFrame) -> tuple[pd.DataFrame, list[int]]:
    """Extract sites from assets dataframe.

    Args:
        assets: Dataframe of assets with 'longitude' and 'latitude' column.

    Returns:
        DataFrame of `n` unique Sites and list of `len(assets)` indices
        mapping each asset to its corresponding site.
    """
    site_keys = list(zip(assets['longitude'], assets['latitude']))
    group_indices, unique_keys = pd.factorize(site_keys)
    unique_sites = pd.DataFrame(unique_keys.tolist(),
                                columns=['longitude', 'latitude'])
    return unique_sites, group_indices.tolist()


def _normalize_tags(df: pd.DataFrame,
                    asset_cols: list[str],
                    tag_cols: list[str]) -> tuple[pd.DataFrame,
                                                  pd.DataFrame,
                                                  pd.DataFrame]:
    """Split a DataFrame into asset values and normalized tags."""
    asset_df = df[asset_cols].copy()

    # Melt tag columns into long format
    tag_df = (
        df[tag_cols]
        .reset_index(drop=True)
        .melt(ignore_index=False, var_name='type', value_name='name')
        .reset_index().rename(columns={'index': 'asset'})
    )

    tag_table, mapping_df = _normalize_tag_pairs(tag_df)
    return asset_df, tag_table, mapping_df


def _normalize_tag_pairs(
        tag_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalize (type, name) pairs using pd.factorize."""
    tag_idx, unique_tags = pd.factorize(
        list(zip(tag_df['type'], tag_df['name'])))
    tag_table = pd.DataFrame(unique_tags.tolist(), columns=['type', 'name'])

    mapping_df = tag_df[['asset', 'type']].copy()
    mapping_df['aggregationtag'] = tag_idx
    mapping_df.rename(columns={'type': 'aggregationtype'}, inplace=True)

    return tag_table, mapping_df


def _try_convert_numeric(df: pd.DataFrame) -> pd.DataFrame:
    """Try to convert all columns in the DataFrame to numeric types.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with numeric columns converted, non-numeric columns intact.
    """
    out = df.copy()
    for c in out.columns:
        try:
            s = pd.to_numeric(out[c])
        except (ValueError, TypeError):
            continue  # leave non-numeric columns alone

        # If it's float but all non-NA values are integer-like, use Int64
        if pd.api.types.is_float_dtype(s) and s.dropna().mod(1).eq(0).all():
            s = s.astype("Int64")

        out[c] = s
    return out


def parse_exposure(file: TextIO
                   ) -> tuple[ExposureModel, pd.DataFrame, pd.DataFrame,
                              pd.DataFrame, pd.DataFrame]:
    """Parse exposure file and return database-ready DataFrames.

    Args:
        file: Open file object containing exposure XML.

    Returns:
        Tuple containing:
        - ExposureModel pydantic object
        - Sites DataFrame ready for database insertion
        - Assets DataFrame ready for database insertion
        - AggregationTags DataFrame ready for database insertion
        - Asset-Tag association DataFrame ready for database insertion
    """
    # Parse the exposure file
    exposure_model, assets_path = parse_exposure_metadata(file)

    with open(assets_path, 'r') as f:
        assets = parse_exposure_assets(f, exposure_model.aggregationtypes)

    # Get aggregation types from the DataFrame
    aggregation_types = [
        x for x in assets.columns if x not in list(
            ASSETS_COLS_MAPPING.values()) + ['longitude', 'latitude']]

    # Asset columns for database insertion
    asset_cols = list(ASSETS_COLS_MAPPING.values()) + ['_site_oid']

    # Extract sites and get site mapping
    sites, assets['_site_oid'] = _extract_sites(assets)

    # Normalize aggregation tags
    assets_clean, aggregationtags, assoc_table = _normalize_tags(
        assets, asset_cols, aggregation_types)

    # convert columns to numeric if possible, keeps non-numeric columns intact

    assets_clean = _try_convert_numeric(assets_clean)
    sites = _try_convert_numeric(sites)
    aggregationtags = _try_convert_numeric(aggregationtags)
    assoc_table = _try_convert_numeric(assoc_table)

    return exposure_model, sites, assets_clean, aggregationtags, assoc_table
