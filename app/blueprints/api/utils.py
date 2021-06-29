from datamodel import Site

from flask import current_app

import configparser
import io
from typing import TextIO, Tuple
import pandas as pd
import ast


def read_asset_csv(file: TextIO) -> pd.DataFrame:
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
                            'structural': 'structuralvalue_value'
                            })
    if 'CantonGemeinde' in df:
        df = df.rename(columns={'CantonGemeinde': '_municipality_oid'})
        df['_municipality_oid'] = df['_municipality_oid'].apply(
            lambda x: x[2:])

    if 'CantonGemeindePC' in df:
        df = df.rename(columns={'CantonGemeindePC': '_postalcode_oid'})
        df['_postalcode_oid'] = df['_postalcode_oid'].apply(lambda x: x[-4:])

    return df


def sites_from_assets(assets: pd.DataFrame) -> Tuple[list, list]:
    """
    Extract sites from assets dataframe

    :params assets: Dataframe of assets with 'lon' and 'lat' column
    :returns:       list of Site objects and list of group numbers for dataframe rows
    """
    # group by sites
    site_groups = assets.groupby(['lon', 'lat'])

    all_sites = []

    # create site models
    for name, _ in site_groups:
        site = Site(longitude_value=name[0],
                    latitude_value=name[1],
                    _assetcollection_oid=int(assets.loc[0, '_assetcollection_oid']))
        all_sites.append(site)

    # return sites alongside with group index
    return all_sites, site_groups.grouper.group_info[0]


def ini_to_dict(filepointer: io.BytesIO) -> dict:
    # make sure ini has at least one section
    byte_str = filepointer.read()

    file_content = '[dummy_section]\n' + byte_str.decode('UTF-8')

    # read ini
    config = configparser.RawConfigParser()
    config.read_string(file_content)

    # parse to dict
    mydict = {}
    for k, v in {s: dict(config.items(s)) for s in config.sections()}.items():
        mydict.update({key: value for key, value in v.items()})

    # try and parse values to appropriate types
    for k, v in mydict.items():
        try:
            mydict[k] = ast.literal_eval(v)
        except:
            pass

    return mydict


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


def createFP(template_name, **kwargs):
    """ create file pointer """
    sio = io.StringIO()
    template = current_app.jinja_env.get_template(template_name)
    template.stream(**kwargs).dump(sio)
    sio.seek(0)
    sio.name = template_name.rsplit('/', 1)[-1]
    return sio
