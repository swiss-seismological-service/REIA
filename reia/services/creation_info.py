"""Utility functions for populating creation info fields."""

try:
    import tomllib  # noqa
except ImportError:
    import tomli as tomllib  # Python < 3.11

import getpass
import socket
from datetime import datetime, timezone
from pathlib import Path

from reia.config.settings import get_settings


def get_system_author() -> str:
    """Get system author in user@hostname format.

    Returns:
        String in format 'user@hostname' or 'REIA-System' as fallback.
    """
    try:
        user = getpass.getuser()
        hostname = socket.gethostname()
        return f"{user}@{hostname}"
    except Exception:
        return 'REIA-System'


def get_reia_version() -> str:
    """Get REIA version from pyproject.toml.

    Returns:
        Version string from pyproject.toml or 'unknown' if not found.
    """
    try:
        # Find pyproject.toml - could be in package root or current directory
        possible_paths = [
            Path(__file__).parent.parent.parent / "pyproject.toml",  # Package
            Path.cwd() / "pyproject.toml",  # Current directory
        ]

        for toml_path in possible_paths:
            if toml_path.exists():
                with open(toml_path, 'rb') as f:
                    data = tomllib.load(f)
                    return data.get('project', {}).get('version', 'unknown')

        return 'unknown'
    except Exception:
        return 'unknown'


def get_creation_info_values() -> dict[str, str]:
    """Get standardized creation info values for REIA objects.

    Returns:
        Dictionary with creation info fields populated with system values.
    """
    settings = get_settings()

    return {
        'creationinfo_author': get_system_author(),
        'creationinfo_agencyid': settings.agency_id,
        'creationinfo_version': get_reia_version(),
        'creationinfo_creationtime': datetime.now(timezone.utc).replace(
            microsecond=0
        )
    }


def populate_creation_info(obj) -> None:
    """Populate creation info fields on an object in-place.

    Args:
        obj: Object with creation info fields to populate.
    """
    creation_info = get_creation_info_values()

    for field_name, value in creation_info.items():
        if hasattr(obj, field_name):
            setattr(obj, field_name, value)
