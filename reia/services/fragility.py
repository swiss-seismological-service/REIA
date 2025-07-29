import io
from pathlib import Path

from reia.io.read import parse_fragility
from reia.repositories.fragility import FragilityModelRepository
from reia.repositories.types import SessionType
from reia.schemas.fragility_schemas import FragilityModel
from reia.utils import create_file_pointer_jinja


def add_fragility_from_file(
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
    with open(file_path, 'r') as f:
        model = parse_fragility(f)
    model.name = name

    fragility_model = FragilityModelRepository.create(session, model)
    return fragility_model


def create_fragility_file(
        session: SessionType,
        fragility_oid: int,
        output_path: Path) -> bool:
    """Export fragility model from data storage layer to disk file.

    Args:
        session: Database session.
        fragility_oid: ID of the fragility model.
        output_path: Path where to save the file.

    Returns:
        True if file was created successfully.
    """
    output_path = output_path.with_suffix('.xml')

    file_pointer = create_fragility_input(session, fragility_oid)

    output_path.parent.mkdir(exist_ok=True)
    output_path.open('w').write(file_pointer.getvalue())

    return output_path.exists()


def create_fragility_input(
        session: SessionType,
        fragility_model_oid: int,
        template_name: Path = Path('reia/templates/fragility.xml')) \
        -> io.StringIO:
    """Generate fragility model from data storage layer to in-memory file.

    Args:
        session: Database session.
        fragility_model_oid: ID of the FragilityModel to be used.
        template_name: Template to be used for the fragility file.

    Returns:
        In-memory file object for fragility input.
    """
    fragility_model = FragilityModelRepository.get_by_id(
        session, fragility_model_oid)

    data = fragility_model.model_dump(mode='json')

    return create_file_pointer_jinja(template_name, data=data)
