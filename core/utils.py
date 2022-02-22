import io
import ast
import configparser

import pandas as pd
from typing import Tuple, TextIO
from jinja2 import Template, select_autoescape

from esloss.datamodel import Site


def sites_from_assets(assets: pd.DataFrame) -> Tuple[list, list]:
    """
    Extract sites from assets dataframe

    :params assets: Dataframe of assets with 'lon' and 'lat' column
    :returns:       lists of Site objects and group numbers for dataframe rows
    """
    # group by sites
    site_groups = assets.groupby(['lon', 'lat'])

    all_sites = []

    # create site models
    for name, _ in site_groups:
        site = Site(
            longitude_value=name[0],
            latitude_value=name[1],
            _assetcollection_oid=int(
                assets.iloc[0]['_assetcollection_oid']))
        all_sites.append(site)

    # return sites alongside with group index
    return all_sites, site_groups.grouper.group_info[0]


def ini_to_dict(file: TextIO) -> dict:
    # make sure ini has at least one section
    content = file.read()

    file_content = '[dummy_section]\n' + content

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
        except BaseException:
            pass

    return mydict


def create_file_pointer(template_name: str, **kwargs) -> io.StringIO:
    """ create file pointer """
    sio = io.StringIO()
    with open(template_name) as t:
        template = Template(t.read(), autoescape=select_autoescape())
    template.stream(**kwargs).dump(sio)
    sio.seek(0)
    sio.name = template_name.rsplit('/', 1)[-1]
    return sio
