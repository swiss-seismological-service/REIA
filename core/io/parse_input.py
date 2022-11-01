import configparser
import os
import xml.etree.ElementTree as ET
from collections import Counter
from typing import TextIO, Tuple

import pandas as pd

from core.utils import CalculationBranchSettings, flatten_config

ASSETS_COLS_MAPPING = {'taxonomy': 'taxonomy_concept',
                       'number': 'buildingcount',
                       'contents': 'contentsvalue',
                       'day': 'dayoccupancy',
                       'night': 'nightoccupancy',
                       'transit': 'transitoccupancy',
                       'structural': 'structuralvalue',
                       'nonstructural': 'nonstructuralvalue',
                       'business_interruption': 'businessinterruptionvalue'
                       }

VULNERABILITY_FK_MAPPING = {
    'structural_vulnerability_file': '_structuralvulnerabilitymodel_oid',
    'contents_vulnerability_file': '_contentsvulnerabilitymodel_oid',
    'occupants_vulnerability_file': '_occupantsvulnerabilitymodel_oid',
    'nonstructural_vulnerability_file': '_nonstructuralvulnerabilitymodel_oid',
    'business_interruption_vulnerability_file':
    '_businessinterruptionvulnerabilitymodel_oid'}

FRAGILITY_FK_MAPPING = {
    'structural_fragility_file': '_structuralfragilitymodel_oid',
    'contents_fragility_file': '_contentsfragilitymodel_oid',
    'nonstructural_fragility_file': '_nonstructuralfragilitymodel_oid',
    'business_interruption_fragility_file':
    '_businessinterruptionfragilitymodel_oid'}


def parse_assets(file: TextIO, tagnames: list[str]) -> pd.DataFrame:
    """
    Reads an exposure file with assets into a dataframe

    :params file:   csv file object with headers (Input OpenQuake):
                    id,lon,lat,taxonomy,number,structural,contents,day(
                    CantonGemeinde,CantonGemeindePC, ...)

    :returns:       df with columns for datamodel.Assets object + lat and lon
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


def parse_exposure(file: TextIO) -> Tuple[dict, pd.DataFrame]:
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

    tagnames = root.find('exposureModel/tagNames').text.split(' ')

    asset_csv = root.find('exposureModel/assets').text
    asset_csv = os.path.join(os.path.dirname(file.name), asset_csv)

    with open(asset_csv, 'r') as f:
        assets = parse_assets(f, tagnames)

    return model, assets


def parse_vulnerability(file: TextIO) -> dict:
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
        model['losscategory'] = child.attrib['lossCategory']
        model['publicid'] = child.attrib['id']
    model['description'] = root.find('vulnerabilityModel/description').text

    # read values for VulnerabilityFunctions
    for vF in root.findall('vulnerabilityModel/vulnerabilityFunction'):
        fun = {}
        fun['taxonomy_concept'] = vF.attrib['id']
        fun['distribution'] = vF.attrib['dist']
        fun['intensitymeasuretype'] = vF.find('imls').attrib['imt']

        imls = vF.find('imls').text.split(' ')
        meanLRs = vF.find('meanLRs').text.split(' ')
        covLRs = vF.find('covLRs').text.split(' ')

        fun['lossratios'] = []
        for i, m, c in zip(imls, meanLRs, covLRs):
            fun['lossratios'].append({'intensitymeasurelevel': i,
                                      'mean': m,
                                      'coefficientofvariation': c})

        model['vulnerabilityfunctions'].append(fun)

    return model


def parse_calculation(branch_settings: list[CalculationBranchSettings]) \
        -> tuple[dict, list[dict]]:
    """
    Parses multiple `esloss` OQ calculation files to the structure of a
    `Calculation` and multiple `CalculationBranch` objects respectively.
    """
    calculation = {}
    calculation_branches = []

    for settings in branch_settings:
        calculation_branch_setting = {}

        # clean and flatten config
        flat_job = configparser.ConfigParser()
        flat_job.read_dict(settings.config)
        for s in ['vulnerability', 'exposure', 'hazard', 'fragility']:
            flat_job.remove_section(s)
        flat_job = flatten_config(flat_job)

        # CALCULATION SETTINGS ###########################################
        aggregateby = flat_job.pop('aggregate_by', None)
        aggregateby = [x.strip() for x in aggregateby.split(',')]

        # validate that main calculation settings are the same in all branches
        if 'calculation_mode' in calculation and \
                flat_job['calculation_mode'] != calculation[
                    'calculation_mode']:
            raise ValueError('Calculation mode must be the same '
                             'in all calculation branches.')

        if 'aggregate_by' in calculation and \
                Counter(aggregateby) != Counter(calculation['aggregate_by']):
            raise ValueError('Aggregation keys must be the same '
                             'in all calculation branches.')

        if '_assetcollection_oid' in calculation and \
                calculation['_assetcollection_oid'] != \
                settings.config['exposure']['exposure_file']:
            raise ValueError('AssetCollection must be the same '
                             'in all calculation branches.')

        # assign all settings to dict
        calculation['calculation_mode'] = flat_job.pop('calculation_mode')
        calculation['description'] = flat_job.pop('description', None)
        calculation['aggregate_by'] = aggregateby
        calculation['_assetcollection_oid'] = \
            settings.config['exposure']['exposure_file']

        # BRANCH SETTINGS ###########################################

        # vulnerability / fragility functions
        if calculation['calculation_mode'] == 'scenario_risk':
            for k, v in settings.config['vulnerability'].items():
                calculation_branch_setting[VULNERABILITY_FK_MAPPING[k]] = v

        if calculation['calculation_mode'] == 'scenario_damage':
            for k, v in settings.config['fragility'].items():
                calculation_branch_setting[FRAGILITY_FK_MAPPING[k]] = v

        # save general config values
        calculation_branch_setting['config'] = flat_job

        # count number of gmfs of input
        gmfs = pd.read_csv(settings.config['hazard']['gmfs_csv'])
        calculation_branch_setting['config'][
            'number_of_ground_motion_fields'] = len(gmfs.eid.unique())

        # add weight
        calculation_branch_setting['weight'] = settings.weight

        calculation_branches.append(calculation_branch_setting)

    return (calculation, calculation_branches)
