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


def parse_assets(file: TextIO, tagnames: list[str]) -> pd.DataFrame:
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


def parse_exposure(file: TextIO) -> tuple[ExposureModel, pd.DataFrame]:
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

    asset_csv = root.find('exposureModel/assets').text
    asset_csv = os.path.join(os.path.dirname(file.name), asset_csv)

    with open(asset_csv, 'r') as f:
        assets = parse_assets(f, model['aggregationtypes'])

    model = ExposureModel.model_validate(model)
    return model, assets


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


def combine_assets(files: list[str]) -> pd.DataFrame:
    combined_assets = pd.DataFrame()

    for exposure in files:
        with open(exposure, 'r') as f:
            _, assets = parse_exposure(f)
            combined_assets = pd.concat([combined_assets, assets])
    return combined_assets
