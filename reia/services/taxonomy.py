import io
from pathlib import Path

import pandas as pd

from reia.datamodel.fragility import TaxonomyMap
from reia.io.write import create_taxonomymap_buffer
from reia.repositories.fragility import (MappingRepository,
                                         TaxonomyMapRepository)
from reia.repositories.types import SessionType


def add_taxonomymap_from_file(session: SessionType,
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

    taxonomy_map = TaxonomyMapRepository.insert_many(session, mapping, name)
    return taxonomy_map


def create_taxonomymap_file(
        session: SessionType,
        taxonomymap_oid: int,
        output_path: Path) -> bool:
    """Export taxonomy mapping from data storage layer to disk file.

    Args:
        session: Database session.
        taxonomymap_oid: ID of the taxonomy mapping.
        output_path: Path where to save the file.

    Returns:
        True if file was created successfully.
    """
    output_path = output_path.with_suffix('.csv')

    file_pointer = create_taxonomymap_input(session, taxonomymap_oid)

    output_path.parent.mkdir(exist_ok=True)
    output_path.open('w').write(file_pointer.getvalue())

    return output_path.exists()


def create_taxonomymap_input(session: SessionType,
                             taxonomymap_oid: int,
                             name: str = 'taxonomy_mapping.csv'
                             ) -> io.StringIO:
    """Generate taxonomy mapping from data storage layer to in-memory file.

    Args:
        session: Database session.
        taxonomymap_oid: ID of the TaxonomyMap to be used.
        name: Name of the file to be created.

    Returns:
        In-memory file object for taxonomy mapping CSV.
    """
    mappings = MappingRepository.get_by_taxonomymap_oid(session,
                                                        taxonomymap_oid)
    return create_taxonomymap_buffer(mappings, name)
