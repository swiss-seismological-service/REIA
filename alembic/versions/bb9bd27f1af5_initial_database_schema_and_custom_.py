"""Initial database schema and custom functions

Revision ID: bb9bd27f1af5
Revises:
Create Date: 2025-07-31 10:18:10.972965

"""
import sys
from pathlib import Path
from typing import Sequence, Union

import sqlalchemy as sa  # noqa
from alembic import op  # noqa

# Add migration_utils to path
sys.path.append(str(Path(__file__).parent.parent))

try:
    from migration_utils import apply_reia_sql_scripts, remove_reia_sql_scripts
except ImportError:
    # Fallback for when running from installed package
    import os
    import importlib.util
    utils_path = Path(__file__).parent.parent / "migration_utils.py"
    spec = importlib.util.spec_from_file_location("migration_utils", utils_path)
    migration_utils = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration_utils)
    apply_reia_sql_scripts = migration_utils.apply_reia_sql_scripts
    remove_reia_sql_scripts = migration_utils.remove_reia_sql_scripts

# revision identifiers, used by Alembic.
revision: str = 'bb9bd27f1af5'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Upgrade schema."""
    print("🚀 Creating REIA database schema...")

    # Create all SQLAlchemy tables
    # Import all models to ensure they're registered
    from reia.datamodel import (asset, calculations, exposure,  # noqa
                                fragility, lossvalues, vulnerability)
    from reia.datamodel.base import ORMBase
    from reia.repositories import engine

    # Create all tables defined by SQLAlchemy models
    ORMBase.metadata.create_all(engine)
    print("✅ SQLAlchemy tables created")

    # Apply custom SQL scripts using utility functions
    apply_reia_sql_scripts()

    print("🎉 Database schema created successfully with all custom functions!")


def downgrade() -> None:
    """Downgrade schema."""
    print("🔄 Removing REIA database schema...")

    # Remove custom database objects using utility functions
    remove_reia_sql_scripts()

    # Drop all SQLAlchemy tables
    # Import all models to ensure they're registered
    from reia.datamodel import (asset, calculations, exposure,  # noqa
                                fragility, lossvalues, vulnerability)
    from reia.datamodel.base import ORMBase
    from reia.repositories import engine

    ORMBase.metadata.drop_all(engine)
    print("✅ SQLAlchemy tables dropped")

    print("🎉 Database schema removed successfully!")
