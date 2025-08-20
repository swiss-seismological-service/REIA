import io
from pathlib import Path

from reia.io.read import parse_exposure, parse_shapefile_geometries
from reia.io.write import create_exposure_buffer
from reia.repositories.asset import (AggregationGeometryRepository,
                                     AssetRepository, ExposureModelRepository)
from reia.repositories.types import SessionType
from reia.schemas.exposure_schema import ExposureModel
from reia.services import DataService
from reia.services.logger import LoggerService


class ExposureService(DataService):
    logger = LoggerService.get_logger(__name__)

    @classmethod
    def import_from_file(
            cls,
            session: SessionType,
            file_path: Path,
            name: str) -> ExposureModel:
        """Load exposure model from file into data storage layer.

        Args:
            session: Database session.
            file_path: Path to the exposure file.
            name: Name for the exposure model.

        Returns:
            Created ExposureModel.
        """
        cls.logger.info(f"Importing exposure model '{name}' from {file_path}")
        with open(file_path, 'r') as f:
            exposure, sites, assets, aggregationtags, assoc_table = \
                parse_exposure(f)

        exposure.name = name

        exposuremodel = ExposureModelRepository.create(session, exposure)
        cls.logger.debug(f"Created exposure model with OID {exposuremodel.oid}")

        # Add the exposure model OID to all DataFrames
        sites['_exposuremodel_oid'] = exposuremodel.oid
        assets['_exposuremodel_oid'] = exposuremodel.oid
        aggregationtags['_exposuremodel_oid'] = exposuremodel.oid

        AssetRepository.insert_from_exposuremodel(
            session, sites, assets, aggregationtags, assoc_table)

        cls.logger.info(
            f"Successfully imported exposure model '{name}' "
            f"with {len(assets)} assets")
        return exposuremodel

    @classmethod
    def export_to_file(
            cls,
            session: SessionType,
            oid: int,
            file_path: str) -> tuple[str, str]:
        """Export exposure model from data storage layer to disk files.

        Args:
            session: Database session.
            oid: ID of the exposure model.
            file_path: Base path where to save the files.

        Returns:
            Tuple of created filenames (XML path, CSV path).
        """
        output_path = Path(file_path)
        p_xml = output_path.with_suffix('.xml')
        p_csv = output_path.with_suffix('.csv')

        fp_xml, fp_csv = cls.export_to_buffer(session, oid)

        p_xml.parent.mkdir(exist_ok=True)
        p_xml.open('w').write(fp_xml.getvalue())
        p_csv.open('w').write(fp_csv.getvalue())

        return str(p_xml), str(p_csv)

    @classmethod
    def export_to_buffer(cls, session: SessionType, oid: int) -> \
            tuple[io.StringIO, io.StringIO]:
        """Generate exposure model from data storage layer to in-memory files.

        Args:
            session: Database session.
            oid: ID of the ExposureModel to be used.

        Returns:
            In-memory file objects for exposure XML and assets CSV.
        """
        exposuremodel = ExposureModelRepository.get_by_id(session, oid)
        exposure_df = AssetRepository.get_by_exposuremodel(session, oid)

        template_name = Path('reia/templates/exposure.xml')
        assets_csv_name = Path('exposure_assets.csv')

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
