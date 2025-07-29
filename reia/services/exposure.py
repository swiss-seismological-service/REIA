import io
from pathlib import Path

from reia.io.read import parse_exposure, parse_shapefile_geometries
from reia.io.write import create_exposure_buffer
from reia.repositories.asset import (AggregationGeometryRepository,
                                     AssetRepository, ExposureModelRepository)
from reia.repositories.types import SessionType
from reia.schemas.exposure_schema import ExposureModel


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
        Created ExposureModel with counts of assets and sites.
    """
    with open(file_path, 'r') as f:
        exposure, sites, assets, aggregationtags, assoc_table = \
            parse_exposure(f)

    exposure.name = name

    exposuremodel = ExposureModelRepository.create(session, exposure)

    # Add the exposure model OID to all DataFrames
    sites['_exposuremodel_oid'] = exposuremodel.oid
    assets['_exposuremodel_oid'] = exposuremodel.oid
    aggregationtags['_exposuremodel_oid'] = exposuremodel.oid

    assets_oids, sites_oids = AssetRepository.insert_from_exposuremodel(
        session, sites, assets, aggregationtags, assoc_table)

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

    fp_xml, fp_csv = create_exposure_inputs(
        session, exposure_oid, assets_csv_name=p_csv)

    p_xml.parent.mkdir(exist_ok=True)
    p_xml.open('w').write(fp_xml.getvalue())
    p_csv.open('w').write(fp_csv.getvalue())

    return p_xml.exists() and p_csv.exists()


def create_exposure_inputs(
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
