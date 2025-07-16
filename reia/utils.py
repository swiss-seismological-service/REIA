import ast
import configparser
import io
import sys
from pathlib import Path
from typing import Any, TextIO, Tuple

import pandas as pd
from jinja2 import Template, select_autoescape


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


def sites_from_assets(assets: pd.DataFrame) \
        -> Tuple[pd.DataFrame, list[int]]:
    """
    Extract sites from assets dataframe

    :params assets: Dataframe of assets with 'longitude' and 'latitude' column
    :returns:       lists of Site objects and group numbers for dataframe rows
    """
    site_keys = list(zip(assets['longitude'], assets['latitude']))
    group_indices, unique_keys = pd.factorize(site_keys)
    unique_sites = pd.DataFrame(unique_keys.tolist(),
                                columns=['longitude', 'latitude'])
    return unique_sites, group_indices.tolist()


def split_assets_and_tags(df: pd.DataFrame,
                          asset_cols: list[str],
                          tag_cols: list[str]) \
        -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Split a DataFrame into asset values and tags.

    :param df: DataFrame containing asset values and tags.
    :param asset_cols: List of columns that contain asset values.
    :param tag_cols: List of columns that contain tags.

    :returns: Tuple of DataFrames:
        - First DataFrame: asset values only.
        - Second DataFrame: melted tags with 'type' and 'name'.
        - Third DataFrame: mapping of asset to tag indices."""
    # First DataFrame: asset values only
    asset_df = df[asset_cols].copy()

    # Melt the tag columns, preserving the original row index
    df = df.reset_index(drop=True)
    tag_df = df[tag_cols].reset_index().melt(
        id_vars=['index'],
        value_vars=tag_cols,
        var_name='type',
        value_name='name'
    ).rename(columns={'index': 'asset'})

    tag_df, mapping_df = normalize_tags(tag_df)

    return asset_df, tag_df, mapping_df


def normalize_tags(tag_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Use pd.factorize to normalize (type, name) pairs.
    Returns:
    - tag_table: unique (type, name)
    - mapping_table: rows with asset, aggregationtag, aggregationtype
    """
    # Combine 'type' and 'name' as tuples for uniqueness
    keys = list(zip(tag_df['type'], tag_df['name']))

    # Factorize to get unique (type, name) combos and mapping indices
    tag_indices, unique_keys = pd.factorize(keys)

    # Build tag_table from unique_keys
    tag_table = pd.DataFrame(unique_keys.tolist(), columns=['type', 'name'])

    # Build mapping_table
    mapping_table = tag_df[['asset', 'type']].copy()
    mapping_table['aggregationtag'] = tag_indices
    mapping_table.rename(columns={'type': 'aggregationtype'}, inplace=True)

    return tag_table, mapping_table


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
