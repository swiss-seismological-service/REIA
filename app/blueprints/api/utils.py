from datamodel import Site
import pandas as pd


def read_asset_csv(file):
    """ Reads an exposure file with assets into a dataframe

    :args: file: csv file object with the following headers:
    id,lon,lat,taxonomy,number,structural,contents,day(
        CantonGemeinde,CantonGemeindePC, ...)

    :returns: dataframe: with columns compatible with the datamodel.Assets object + lat and lon
     """

    df = pd.read_csv(file, index_col='id')

    df = df.rename(columns={'taxonomy': 'taxonomy_concept',
                            'number': 'buildingCount',
                            'contents': 'contentvalue_value',
                            'day': 'occupancydaytime_value',
                            'structural': 'structuralvalue_value'
                            })
    if 'CantonGemeinde' in df:
        df = df.rename(columns={'CantonGemeinde': '_municipality_oid'})
        df['_municipality_oid'] = df['_municipality_oid'].apply(
            lambda x: x[2:])

    if 'CantonGemeindePC' in df:
        df = df.rename(columns={'CantonGemeindePC': '_postalCode_oid'})
        df['_postalCode_oid'] = df['_postalCode_oid'].apply(lambda x: x[-4:])

    return df


def sites_from_asset_dataframe(assets_df):
    # group by sites
    site_groups = assets_df.groupby(['lon', 'lat'])

    all_sites = []

    # create site models
    for name, _ in site_groups:
        site = Site(longitude_value=name[0],
                    latitude_value=name[1],
                    _assetCollection_oid=int(assets_df.loc[0, '_assetCollection_oid']))
        all_sites.append(site)

    # return sites alongside with group index
    return all_sites, site_groups.grouper.group_info[0]
