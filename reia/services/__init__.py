import io
from abc import ABC, abstractmethod
from pathlib import Path

from reia.repositories.types import SessionType
from reia.schemas.base import Model


class DataService(ABC):
    @classmethod
    @abstractmethod
    def import_from_file(
            cls,
            session: SessionType,
            file_path: Path,
            name: str) -> Model:
        """Load data from a file and store it via the repository."""
        pass

    @classmethod
    @abstractmethod
    def export_to_file(
            cls,
            session: SessionType,
            oid: int,
            file_path: str) -> str:
        """Retrieve data from the repository and save it to a file."""
        pass

    @classmethod
    @abstractmethod
    def export_to_buffer(cls, session: SessionType, oid: int) -> io.StringIO:
        """Return a file-like object containing the data."""
        pass
