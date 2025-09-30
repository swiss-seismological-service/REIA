import ast
import configparser
import io
import re
import sys
import tempfile
from copy import deepcopy
from datetime import datetime
from io import StringIO
from pathlib import Path
from typing import Any

import pandas as pd
import typer
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


def flatten_config(config: configparser.ConfigParser) -> dict:
    """Flatten a configparser object to a single dictionary.

    Args:
        config: ConfigParser object to flatten.
    Returns:
        Flattened dictionary with all sections merged.
    """
    config = deepcopy(config)
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


def clean_config(config: configparser.ConfigParser) -> dict:
    """
    Cleans and flattens a configparser object by removing openquake
    file inputs and only keeping the oqparam inputs.

    Args:
        config: ConfigParser object to clean.
    Returns:
        Cleaned and flattened ConfigParser object.
    """
    config = deepcopy(config)
    if config.has_section('hazard_source'):
        config.remove_section('hazard_source')

    flat_config = flatten_config(config)

    keys_to_remove = [
        'exposure_file',
        'structural_fragility_file',
        'nonstructural_fragility_file',
        'contents_fragility_file',
        'business_interruption_fragility_file',
        'structural_vulnerability_file',
        'nonstructural_vulnerability_file',
        'contents_vulnerability_file',
        'business_interruption_vulnerability_file',
        'occupants_vulnerability_file',
        'taxonomy_mapping_csv',
        'gmfs_csv',
        'sites_csv']

    for key in keys_to_remove:
        flat_config.pop(key, None)

    return flat_config


def create_file_buffer_jinja(template_name: str, **kwargs) -> io.StringIO:
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


def create_file_buffer_configparser(settings: configparser.ConfigParser,
                                    name: str) -> io.StringIO:
    job_ini = io.StringIO()
    settings.write(job_ini)
    job_ini.seek(0)
    job_ini.name = name

    return job_ini


def create_file_buffer_dataframe(df: pd.DataFrame,
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


def display_table(title: str, headers: list[str], rows: list[list],
                  widths: list[int] = None) -> None:
    """Display a formatted table with automatic column width calculation.

    Args:
        title: Table title to display above the table.
        headers: List of column headers.
        rows: List of rows, where each row is a list of values.
        widths: Optional list of column widths. If not provided,
                calculates automatically.
    """
    if not rows:
        typer.echo(title)
        typer.echo("No items found.")
        return

    # Calculate column widths if not provided
    if widths is None:
        widths = []
        for i in range(len(headers)):
            # Get max width needed for this column (header vs data)
            header_width = len(headers[i])
            data_width = max(len(str(row[i])) for row in rows)
            widths.append(max(header_width, data_width) + 2)  # Add padding

    # Format and display title
    typer.echo(title)

    # Format and display header
    header_format = ' '.join(f'{{:<{w}}}' for w in widths)
    typer.echo(header_format.format(*headers))

    # Format and display rows
    row_format = ' '.join(f'{{:<{w}}}' for w in widths)
    for row in rows:
        # Handle None values and format datetime objects
        formatted_row = []
        for item in row:
            if item is None:
                formatted_row.append('')
            elif isinstance(item, datetime):
                formatted_row.append(item.strftime('%Y-%m-%d %H:%M:%S'))
            else:
                formatted_row.append(str(item))
        typer.echo(row_format.format(*formatted_row))


def write_buffer_temp(
        buffer: StringIO,
        name: str,
        tmp_dir=tempfile.TemporaryDirectory()) -> Path:
    path = Path(tmp_dir.name) / name
    path.write_text(buffer.getvalue(), encoding="utf-8")
    # keep a reference to tmp_dir for as long as you need the file
    return path, tmp_dir
