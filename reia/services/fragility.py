import io
from pathlib import Path

from reia.io.read import parse_fragility
from reia.io.write import create_fragility_buffer
from reia.repositories.fragility import FragilityModelRepository
from reia.repositories.types import SessionType
from reia.schemas.fragility_schemas import FragilityModel
from reia.services import DataService
from reia.services.logger import LoggerService


class FragilityService(DataService):
    logger = LoggerService.get_logger(__name__)

    @classmethod
    def import_from_file(
            cls,
            session: SessionType,
            file_path: Path,
            name: str) -> FragilityModel:
        """Load fragility model from file into data storage layer.

        Args:
            session: Database session.
            file_path: Path to the fragility file.
            name: Name for the fragility model.

        Returns:
            Created FragilityModel.
        """
        cls.logger.info(f"Importing fragility model '{name}' from {file_path}")
        with open(file_path, 'r') as f:
            model = parse_fragility(f)
        model.name = name

        fragility_model = FragilityModelRepository.create(session, model)
        cls.logger.info(f"Successfully imported fragility model '{name}'")
        return fragility_model

    @classmethod
    def export_to_file(
            cls,
            session: SessionType,
            oid: int,
            file_path: str) -> str:
        """Export fragility model from data storage layer to disk file.

        Args:
            session: Database session.
            oid: ID of the fragility model.
            file_path: Path where to save the file.

        Returns:
            The filename of the created file.
        """
        output_path = Path(file_path).with_suffix('.xml')

        file_pointer = cls.export_to_buffer(session, oid)

        output_path.parent.mkdir(exist_ok=True)
        output_path.open('w').write(file_pointer.getvalue())

        return str(output_path)

    @classmethod
    def export_to_buffer(cls, session: SessionType, oid: int) -> io.StringIO:
        """Generate fragility model from data storage layer to in-memory file.

        Args:
            session: Database session.
            oid: ID of the FragilityModel to be used.

        Returns:
            In-memory file object for fragility input.
        """
        fragility_model = FragilityModelRepository.get_by_id(session, oid)
        template_name = Path('reia/templates/fragility.xml')
        return create_fragility_buffer(fragility_model, template_name)
