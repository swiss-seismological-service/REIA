from typing import TextIO
import pandas as pd
import xml.etree.ElementTree as ET


def parse_asset_csv(file: TextIO) -> pd.DataFrame:
    """ 
    Reads an exposure file with assets into a dataframe

    :params file:   csv file object with the following headers (Input for OpenQuake):
                    id,lon,lat,taxonomy,number,structural,contents,day(
                    CantonGemeinde,CantonGemeindePC, ...)

    :returns:       df with columns compatible with the datamodel.Assets object + lat and lon
     """

    df = pd.read_csv(file, index_col='id')

    df = df.rename(columns={'taxonomy': 'taxonomy_concept',
                            'number': 'buildingcount',
                            'contents': 'contentvalue_value',
                            'day': 'occupancydaytime_value',
                            'structural': 'structuralvalue_value',
                            'municipality': '_municipality_oid',
                            'postalcode': '_postalcode_oid'
                            })
    if 'CantonGemeinde' in df:
        df = df.rename(columns={'CantonGemeinde': '_municipality_oid'})
        df['_municipality_oid'] = df['_municipality_oid'].apply(
            lambda x: x[2:])

    if 'CantonGemeindePC' in df:
        df = df.rename(columns={'CantonGemeindePC': '_postalcode_oid'})
        df['_postalcode_oid'] = df['_postalcode_oid'].apply(lambda x: x[-4:])

    return df


def risk_dict_to_lossmodel_dict(risk: dict) -> dict:
    loss_dict = {
        'maincalculationmode': risk.get('calculation_mode', 'scenario_risk'),
        'numberofgroundmotionfields': risk.get('number_of_ground_motion_fields', 100),
        'maximumdistance': risk.get('maximum_distance', None),
        'truncationlevel': risk.get('truncation_level', None),
        'randomseed': risk.get('random_seed', None),
        'masterseed': risk.get('master_seed', None),
        'crosscorrelation': True if risk.get('cross_correlation', 'no') == 'yes' else False,
        'spatialcorrelation': True if risk.get('spatial_correlation', 'no') == 'yes' else False,
        'description': risk.get('description', ''),
    }
    return loss_dict


def parse_oq_vulnerability_file(file) -> dict:
    model = {}
    functions = []

    tree = ET.iterparse(file)

    # strip namespace for easier querying
    for _, el in tree:
        _, _, el.tag = el.tag.rpartition('}')

    root = tree.root

    # read values for VulnerabilityModel
    for child in root:
        model['assetcategory'] = child.attrib['assetCategory']
        model['losscategory'] = child.attrib['lossCategory']
        model['publicid_resourceid'] = child.attrib['id']
    model['description'] = root.find('vulnerabilityModel/description').text

    # read values for VulnerabilityFunctions
    for vF in root.findall('vulnerabilityModel/vulnerabilityFunction'):
        fun = {}
        fun['taxonomy_concept'] = vF.attrib['id']
        fun['distribution'] = vF.attrib['dist']
        fun['intensitymeasuretype'] = vF.find('imls').attrib['imt']
        fun['intensitymeasurelevels'] = vF.find('imls').text.split(' ')
        fun['meanlossratios'] = vF.find('meanLRs').text.split(' ')
        fun['covariancelossratios'] = vF.find('covLRs').text.split(' ')
        functions.append(fun)

    return model, functions


def parse_oq_exposure_file(file) -> dict:
    tree = ET.iterparse(file)

    # strip namespace for easier querying
    for _, el in tree:
        _, _, el.tag = el.tag.rpartition('}')

    root = tree.root

    # read values for AssetCollection
    model = {'costtypes': []}
    for child in root:
        model['publicid_resourceid'] = child.attrib['id']
        model['category'] = child.attrib['category']
        model['taxonomysource'] = child.attrib['taxonomySource']

    model['name'] = root.find('exposureModel/description').text
    model['tagnames'] = root.find('exposureModel/tagNames').text.split()
    model['occupancyperiods'] = root.find(
        'exposureModel/occupancyPeriods').text.split()

    for test in root.findall('exposureModel/conversions/costTypes/costType'):
        model['costtypes'].append(test.attrib['name'])
    return model
