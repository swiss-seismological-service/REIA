import ast
import configparser
import io
import sys
from pathlib import Path
from typing import Any, TextIO, Tuple

import pandas as pd
from jinja2 import Template, select_autoescape

from reia.datamodel.asset import AggregationTag, Site


def import_string(import_name: str, silent: bool = False) -> Any:
    """Imports an object based on a string.  This is useful if you want to
    use import paths as endpoints or something similar.  An import path can
    be specified either in dotted notation (``xml.sax.saxutils.escape``)
    or with a colon as object delimiter (``xml.sax.saxutils:escape``).

    If `silent` is True the return value will be `None` if the import fails.

    :param import_name: the dotted name for the object to import.
    :param silent: if set to `True` import errors are ignored and
                   `None` is returned instead.
    :return: imported object
    """
    import_name = import_name.replace(":", ".")
    try:
        try:
            __import__(import_name)
        except ImportError:
            if "." not in import_name:
                raise
        else:
            return sys.modules[import_name]

        module_name, obj_name = import_name.rsplit(".", 1)
        module = __import__(module_name, globals(), locals(), [obj_name])
        try:
            return getattr(module, obj_name)
        except AttributeError as e:
            raise ImportError(e) from None

    except ImportError as e:
        if not silent:
            raise ImportError(import_name, e).with_traceback(
                sys.exc_info()[2]
            ) from None

    return None


def sites_from_assets(assets: pd.DataFrame) -> Tuple[list[Site], list[int]]:
    """
    Extract sites from assets dataframe

    :params assets: Dataframe of assets with 'longitude' and 'latitude' column
    :returns:       lists of Site objects and group numbers for dataframe rows
    """
    # group by sites
    site_groups = assets.groupby(['longitude', 'latitude'])

    all_sites = []

    # create site models
    for name, _ in site_groups:
        site = Site(
            longitude=name[0],
            latitude=name[1])
        all_sites.append(site)

    # return sites alongside with group index
    return all_sites, site_groups.grouper.group_info[0]


def aggregationtags_from_assets(
    assets: pd.DataFrame, aggregation_type: str,
    existing_tags: list[AggregationTag]) -> \
        Tuple[list[AggregationTag], list[int]]:
    """
    Extract aggregationtags from assets dataframe

    :params assets: Dataframe of assets with 'aggregation_type' column
    :returns:       lists of AggregationTag objects and group numbers for
                    dataframe rows
    """
    existing_tags = {str(t.name): t for t in existing_tags}
    exposuremodel_oid = assets['_exposuremodel_oid'].iloc[0]
    agg_groups = assets.groupby(aggregation_type)

    all_tags = []

    for name, _ in agg_groups:
        if str(name) in existing_tags:
            tag = existing_tags[str(name)]
        else:
            tag = AggregationTag(
                type=aggregation_type,
                name=name,
                _exposuremodel_oid=exposuremodel_oid)
        all_tags.append(tag)
    return all_tags, agg_groups.grouper.group_info[0]


def flatten_config(file: TextIO) -> dict:

    if not isinstance(file, configparser.ConfigParser):
        # make sure ini has at least one section
        content = file.read()
        file_content = '[dummy_section]\n' + content

        # read ini
        config = configparser.RawConfigParser()
        config.read_string(file_content)
    else:
        config = file
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
    with open(Path(get_project_root(), template_name)) as t:
        template = Template(t.read(), autoescape=select_autoescape())
    template.stream(**kwargs).dump(sio)
    sio.seek(0)
    sio.name = template_name.name
    return sio


def get_project_root() -> Path:
    return Path(__file__).parent.parent
