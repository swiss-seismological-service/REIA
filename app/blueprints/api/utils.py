from datamodel import Site
from typing import TextIO, Tuple
import pandas as pd


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
