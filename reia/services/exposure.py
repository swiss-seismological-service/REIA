import io
from pathlib import Path

import pandas as pd

from reia.io import ASSETS_COLS_MAPPING
from reia.io.read import parse_exposure, parse_shapefile_geometries
from reia.io.write import create_exposure_buffer
from reia.repositories.asset import (AggregationGeometryRepository,
                                     AssetRepository, ExposureModelRepository)
from reia.repositories.types import SessionType
from reia.schemas.exposure_schema import ExposureModel


def create_exposure_with_assets(session: SessionType,
                                exposure: ExposureModel,
                                assets: pd.DataFrame) \
        -> tuple[ExposureModel, list[int], list[int]]:
    """Create an exposure model and its associated assets.

    Args:
        session: Database session.
        exposure: ExposureModel schema object.
        assets: DataFrame containing asset data.

    Returns:
        Created ExposureModel with oid and lists of asset and site OIDs.
    """
    exposuremodel = ExposureModelRepository.create(session, exposure)
    assets_oids, sites_oids = create_assets(session, assets, exposuremodel.oid)
    return exposuremodel, assets_oids, sites_oids


def create_assets(session: SessionType,
                  assets: pd.DataFrame,
                  exposure_model_oid: int) \
        -> tuple[list[int], list[int]]:
    """Create assets for an exposure model.

    Args:
        session: Database session.
        assets: DataFrame containing asset data.
        exposure_model_oid: OID of the exposure model.

    Returns:
        List of created asset OIDs and site OIDs.
    """
    # get AggregationTag types
    aggregation_types = [
        x for x in assets.columns if x not in list(
            ASSETS_COLS_MAPPING.values()) + ['longitude', 'latitude']]

    # Asset columns
    asset_cols = list(ASSETS_COLS_MAPPING.values()) + \
        ['_site_oid', '_exposuremodel_oid']

    # assign ExposureModel oid to assets
    assets['_exposuremodel_oid'] = exposure_model_oid

    # create Sites
    sites, assets['_site_oid'] = _extract_sites(assets)
    sites['_exposuremodel_oid'] = exposure_model_oid

    # create AggregationTags
    assets, aggregationtags, assoc_table = _normalize_tags(
        assets, asset_cols, aggregation_types)
    aggregationtags['_exposuremodel_oid'] = exposure_model_oid

    assets_oids, sites_oids = AssetRepository.insert_from_exposuremodel(
        session,
        sites,
        assets,
        aggregationtags,
        assoc_table)

    return assets_oids, sites_oids


def _extract_sites(assets: pd.DataFrame) -> tuple[pd.DataFrame, list[int]]:
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


def _normalize_tags(df: pd.DataFrame,
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

    tag_table, mapping_df = _normalize_tag_pairs(tag_df)
    return asset_df, tag_table, mapping_df


def _normalize_tag_pairs(
        tag_df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Normalize (type, name) pairs using pd.factorize."""
    tag_idx, unique_tags = pd.factorize(
        list(zip(tag_df['type'], tag_df['name'])))
    tag_table = pd.DataFrame(unique_tags.tolist(), columns=['type', 'name'])

    mapping_df = tag_df[['asset', 'type']].copy()
    mapping_df['aggregationtag'] = tag_idx
    mapping_df.rename(columns={'type': 'aggregationtype'}, inplace=True)

    return tag_table, mapping_df


def add_exposure_from_file(
        session: SessionType,
        file_path: Path,
        name: str) -> tuple[ExposureModel, int, int]:
    """Load exposure model from file into data storage layer.

    Args:
        session: Database session.
        file_path: Path to the exposure file.
        name: Name for the exposure model.

    Returns:
        Created ExposureModel with counts.
    """
    with open(file_path, 'r') as f:
        exposure, assets = parse_exposure(f)

    exposure.name = name

    exposuremodel, assets_oids, sites_oids = create_exposure_with_assets(
        session, exposure, assets)

    return exposuremodel, len(assets_oids), len(sites_oids)


def create_exposure_files(
        session: SessionType,
        exposure_oid: int,
        output_path: Path) -> bool:
    """Export exposure model from data storage layer to disk files.

    Args:
        session: Database session.
        exposure_oid: ID of the exposure model.
        output_path: Base path where to save the files.

    Returns:
        True if files were created successfully.
    """
    p_xml = output_path.with_suffix('.xml')
    p_csv = output_path.with_suffix('.csv')

    fp_xml, fp_csv = create_exposure_input(
        session, exposure_oid, assets_csv_name=p_csv)

    p_xml.parent.mkdir(exist_ok=True)
    p_xml.open('w').write(fp_xml.getvalue())
    p_csv.open('w').write(fp_csv.getvalue())

    return p_xml.exists() and p_csv.exists()


def add_geometries_from_shapefile(session: SessionType,
                                  exposure_oid: int,
                                  file_path: Path,
                                  tag_column_name: str,
                                  aggregation_type: str) -> int:
    """Load geometries from shapefile into data storage layer.

    Args:
        session: Database session.
        exposure_oid: ID of the exposure model.
        file_path: Path to the shapefile.
        tag_column_name: Name of the aggregation tag column.
        aggregation_type: Type of the aggregation.

    Returns:
        Number of geometries added.
    """
    gdf = parse_shapefile_geometries(
        file_path, tag_column_name, aggregation_type)

    geometry_ids = AggregationGeometryRepository.insert_many(
        session, exposure_oid, gdf)

    return len(geometry_ids)


def create_exposure_input(
        session: SessionType,
        asset_collection_oid: int,
        template_name: Path = Path('reia/templates/exposure.xml'),
        assets_csv_name: Path = Path('exposure_assets.csv')) \
        -> tuple[io.StringIO, io.StringIO]:
    """Generate exposure model from data storage layer to in-memory files.

    Args:
        session: Database session.
        asset_collection_oid: ID of the ExposureModel to be used.
        template_name: Template to be used for the exposure file.
        assets_csv_name: Name for the assets CSV file.

    Returns:
        In-memory file objects for exposure XML and assets CSV.
    """
    exposuremodel = ExposureModelRepository.get_by_id(
        session, asset_collection_oid)

    exposure_df = AssetRepository.get_by_exposuremodel(
        session, asset_collection_oid)

    return create_exposure_buffer(exposuremodel,
                                  exposure_df,
                                  assets_csv_name,
                                  template_name)
