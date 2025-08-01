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

# Add migration_utils to path for helper functions
sys.path.append(str(Path(__file__).parent.parent))

try:
    from migration_utils import (drop_functions, drop_indexes,
                                 drop_materialized_views, drop_triggers,
                                 execute_sql_file)
except ImportError:
    # Fallback for when running from installed package
    import importlib.util
    utils_path = Path(__file__).parent.parent / "migration_utils.py"
    spec = importlib.util.spec_from_file_location(
        "migration_utils", utils_path)
    migration_utils = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(migration_utils)
    execute_sql_file = migration_utils.execute_sql_file
    drop_indexes = migration_utils.drop_indexes
    drop_triggers = migration_utils.drop_triggers
    drop_functions = migration_utils.drop_functions
    drop_materialized_views = migration_utils.drop_materialized_views

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
    from reia.datamodel import (asset, calculations, fragility,  # noqa
                                lossvalues, vulnerability)
    from reia.datamodel.base import ORMBase
    from reia.db import engine

    # Create all tables defined by SQLAlchemy models
    ORMBase.metadata.create_all(engine)
    print("SQLAlchemy tables created")

    # Apply REIA custom SQL scripts in the correct order
    print("Applying REIA custom SQL scripts...")

    # 1. Create materialized views
    execute_sql_file("materialized_loss_buildings.sql")

    # 2. Create triggers and functions
    execute_sql_file("trigger_refresh_materialized.sql")
    execute_sql_file("trigger_partition_aggregationtags.sql")
    execute_sql_file("trigger_partition_losstype.sql")

    # 3. Create performance indexes
    execute_sql_file("indexes.sql")

    print("All REIA custom SQL scripts applied successfully")
    print("Database schema created successfully with all custom functions!")


def downgrade() -> None:
    """Downgrade schema with proper handling of complex dependencies."""
    print("Removing REIA database schema...")

    # Use the existing database connection
    from reia.db import engine

    with engine.connect() as conn:
        # Drop materialized views first (they depend on tables)
        conn.execute(sa.text(
            "DROP MATERIALIZED VIEW IF EXISTS "
            "loss_buildings_per_municipality CASCADE;"))
        print("Dropped materialized views")

        # Drop custom triggers explicitly
        conn.execute(sa.text("""
            DROP TRIGGER IF EXISTS
            refresh_materialized_loss_buildings_trigger ON loss_asset CASCADE;
            DROP TRIGGER IF EXISTS
            insert_calculation_trigger ON loss_calculation CASCADE;
            DROP TRIGGER IF EXISTS
            insert_aggregationtag_trigger ON loss_exposuremodel CASCADE;
        """))
        print("Dropped custom triggers")

        # Drop custom functions
        conn.execute(sa.text("""
            DROP FUNCTION IF EXISTS
                             refresh_materialized_loss_buildings() CASCADE;
            DROP FUNCTION IF EXISTS calculation_partition_function() CASCADE;
            DROP FUNCTION IF EXISTS aggregationtag_partition_function() CASCADE;
        """))
        print("Dropped custom functions")

        # Drop custom indexes (let PostgreSQL handle dependencies)
        custom_indexes = [
            'idx_aggregationtag_name',
            'idx_aggregationtag_type',
            'idx_assoc_riskvalue_aggregationtag',
            'idx_assoc_riskvalue_aggregationtype',
            'idx_assoc_riskvalue_riskvalue',
            'idx_calculation_status_type',
            'idx_calculationbranch_calculation',
            'idx_riskvalue_oid',
            'idx_riskvalue_type',
            'idx_riskvalue_calculationbranch']
        for index in custom_indexes:
            conn.execute(sa.text(f"DROP INDEX IF EXISTS {index} CASCADE;"))
        print("Dropped custom indexes")

        print("Drop partitioned and regular tables...")

        # Drop all tables with CASCADE to handle partitions and dependencies
        # Get all tables that start with 'loss_' (REIA tables)
        result = conn.execute(sa.text("""
            SELECT tablename FROM pg_tables
            WHERE schemaname = 'public'
            AND tablename LIKE 'loss_%'
            ORDER BY tablename;
        """))

        reia_tables = [row[0] for row in result]
        for table in reia_tables:
            conn.execute(sa.text(f"DROP TABLE IF EXISTS {table} CASCADE;"))

        print(f"Dropped {len(reia_tables)} REIA tables with CASCADE")

        conn.commit()

    print("Database schema completely removed with proper dependency handling")
