import io
from pathlib import Path

import pandas as pd

from reia.io.write import create_taxonomymap_buffer
from reia.repositories.fragility import (MappingRepository,
                                         TaxonomyMapRepository)
from reia.repositories.types import SessionType
from reia.schemas.fragility_schemas import TaxonomyMap
from reia.services import DataService
from reia.services.logger import LoggerService


class TaxonomyService(DataService):
    logger = LoggerService.get_logger(__name__)

    @classmethod
    def import_from_file(
            cls,
            session: SessionType,
            file_path: Path,
            name: str) -> TaxonomyMap:
        """Load taxonomy mapping from file into data storage layer.

        Args:
            session: Database session.
            file_path: Path to the taxonomy mapping CSV file.
            name: Name for the taxonomy mapping.

        Returns:
            Created TaxonomyMap.
        """
        mapping = pd.read_csv(file_path)
        taxonomy_map = TaxonomyMapRepository.insert_many(
            session, mapping, name)
        return taxonomy_map

    @classmethod
    def export_to_file(
            cls,
            session: SessionType,
            oid: int,
            file_path: str) -> str:
        """Export taxonomy mapping from data storage layer to disk file.

        Args:
            session: Database session.
            oid: ID of the taxonomy mapping.
            file_path: Path where to save the file.

        Returns:
            The filename of the created file.
        """
        output_path = Path(file_path).with_suffix('.csv')

        file_pointer = cls.export_to_buffer(session, oid)

        output_path.parent.mkdir(exist_ok=True)
        output_path.open('w').write(file_pointer.getvalue())

        return str(output_path)

    @classmethod
    def export_to_buffer(cls, session: SessionType, oid: int) -> io.StringIO:
        """Generate taxonomy mapping from data storage layer to in-memory file.

        Args:
            session: Database session.
            oid: ID of the TaxonomyMap to be used.

        Returns:
            In-memory file object for taxonomy mapping CSV.
        """
        mappings = MappingRepository.get_by_taxonomymap_oid(session, oid)
        name = 'taxonomy_mapping.csv'
        return create_taxonomymap_buffer(mappings, name)
