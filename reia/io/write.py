import io
from pathlib import Path

import pandas as pd

from reia.io import ASSETS_COLS_MAPPING
from reia.schemas.exposure_schema import ExposureModel
from reia.schemas.fragility_schemas import FragilityModel
from reia.schemas.vulnerability_schemas import VulnerabilityModel
from reia.utils import create_file_buffer_dataframe, create_file_buffer_jinja


def create_exposure_buffer(
        exposure_model: ExposureModel,
        assets_df: pd.DataFrame,
        assets_csv_name: Path = Path('exposure_assets.csv'),
        template_name: Path = Path('reia/templates/exposure.xml')) \
        -> tuple[io.StringIO, io.StringIO]:
    """Generate exposure model from pydantic model and
    DataFrame to in-memory files.

    Args:
        exposure_model: ExposureModel pydantic object.
        assets_df: DataFrame containing asset data with aggregation tags.
        assets_csv_name: Name for the assets CSV file.
        template_name: Template to be used for the exposure file.

    Returns:
        In-memory file objects for exposure XML and assets CSV.
    """
    # Generate XML from template
    data = exposure_model.model_dump(mode='json')
    data['assets_csv_name'] = assets_csv_name.name

    exposure_xml = create_file_buffer_jinja(template_name, data=data)

    # Transform DataFrame for CSV export
    assets_df = assets_df.copy()
    assets_df.index.name = 'id'

    columns_map = {**{'longitude': 'lon', 'latitude': 'lat'},
                   **{v: k for k, v in ASSETS_COLS_MAPPING.items()}}

    assets_df = assets_df.rename(columns=columns_map)

    assets_df = assets_df[[*columns_map.values(),
                           *exposure_model.aggregationtypes]] \
        .dropna(axis=1, how='all') \
        .fillna(0)

    exposure_csv = create_file_buffer_dataframe(
        assets_df, name=assets_csv_name.name)

    return (exposure_xml, exposure_csv)


def create_fragility_buffer(
        fragility_model: FragilityModel,
        template_name: Path = Path('reia/templates/fragility.xml')) \
        -> io.StringIO:
    """Generate fragility model from pydantic model to in-memory file.

    Args:
        fragility_model: FragilityModel pydantic object.
        template_name: Template to be used for the fragility file.

    Returns:
        In-memory file object for fragility XML.
    """
    data = fragility_model.model_dump(mode='json')
    return create_file_buffer_jinja(template_name, data=data)


def create_vulnerability_buffer(
        vulnerability_model: VulnerabilityModel,
        template_name: Path = Path('reia/templates/vulnerability.xml')) \
        -> io.StringIO:
    """Generate vulnerability model from pydantic model to in-memory file.

    Args:
        vulnerability_model: VulnerabilityModel pydantic object.
        template_name: Template to be used for the vulnerability file.

    Returns:
        In-memory file object for vulnerability XML.
    """
    data = vulnerability_model.model_dump(mode='json')
    return create_file_buffer_jinja(template_name, data=data)


def create_taxonomymap_buffer(
        mappings_df: pd.DataFrame,
        name: str = 'taxonomy_mapping.csv') -> io.StringIO:
    """Generate taxonomy mapping from DataFrame to in-memory file.

    Args:
        mappings_df: DataFrame containing taxonomy mappings with columns
                    ['taxonomy', 'conversion', 'weight'].
        name: Name of the file to be created.

    Returns:
        In-memory file object for taxonomy mapping CSV.
    """
    mappings_df = mappings_df[['taxonomy', 'conversion', 'weight']]
    return create_file_buffer_dataframe(mappings_df, name=name, index=False)
