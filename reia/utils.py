import ast
import configparser
import io
import re
import sys
from pathlib import Path
from typing import Any, TextIO

import pandas as pd
from jinja2 import Template, select_autoescape


def import_string(import_name: str, silent: bool = False) -> Any:
    """Imports an object based on a string.

    This is useful if you want to use import paths as endpoints or something
    similar. An import path can be specified either in dotted notation
    (``xml.sax.saxutils.escape``) or with a colon as object delimiter
    (``xml.sax.saxutils:escape``).

    If `silent` is True the return value will be `None` if the import fails.

    Args:
        import_name: The dotted name for the object to import.
        silent: If set to `True` import errors are ignored and
                `None` is returned instead.

    Returns:
        The imported object.
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


def sites_from_assets(assets: pd.DataFrame) -> tuple[pd.DataFrame, list[int]]:
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


def normalize_assets_tags(df: pd.DataFrame,
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

    tag_table, mapping_df = normalize_tags(tag_df)
    return asset_df, tag_table, mapping_df


def normalize_tags(tag_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalize (type, name) pairs using pd.factorize."""
    tag_idx, unique_tags = pd.factorize(
        list(zip(tag_df['type'], tag_df['name'])))
    tag_table = pd.DataFrame(unique_tags.tolist(), columns=['type', 'name'])

    mapping_df = tag_df[['asset', 'type']].copy()
    mapping_df['aggregationtag'] = tag_idx
    mapping_df.rename(columns={'type': 'aggregationtype'}, inplace=True)

    return tag_table, mapping_df


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


def create_file_pointer_jinja(template_name: str, **kwargs) -> io.StringIO:
    """Create file pointer."""
    sio = io.StringIO()
    with open(Path(get_project_root(), template_name)) as t:
        template = Template(t.read(), autoescape=select_autoescape())
    template.stream(**kwargs).dump(sio)
    sio.seek(0)
    sio.name = template_name.name
    return sio


def get_project_root() -> Path:
    return Path(__file__).parent.parent


def create_file_pointer_configparser(settings: configparser.ConfigParser,
                                     name: str) -> io.StringIO:
    job_ini = io.StringIO()
    settings.write(job_ini)
    job_ini.seek(0)
    job_ini.name = name

    return job_ini


def create_file_pointer_dataframe(df: pd.DataFrame,
                                  name: str,
                                  **kwargs) -> io.StringIO:
    """Creates a file pointer for a DataFrame."""
    buffer = io.StringIO()
    df.to_csv(buffer, **kwargs)
    buffer.seek(0)
    buffer.name = name
    return buffer


def clean_array(text: str) -> str:
    return re.sub("\\s\\s+", " ", text).strip()
