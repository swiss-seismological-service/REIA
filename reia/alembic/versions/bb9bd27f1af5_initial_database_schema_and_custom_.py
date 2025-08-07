"""Initial database schema and custom functions

Revision ID: bb9bd27f1af5
Revises:
Create Date: 2025-07-31 10:18:10.972965

"""
from pathlib import Path
from typing import Sequence, Union

import sqlalchemy as sa  # noqa
from alembic import op  # noqa

# revision identifiers, used by Alembic.
revision: str = 'bb9bd27f1af5'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def execute_sql_file(filename: str) -> None:
    """Execute a SQL file from the scripts directory."""
    sql_file = Path(__file__).parent.parent / \
        "scripts" / filename

    if sql_file.exists():
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        op.execute(sql_content)
        print(f"Executed SQL file: {filename}")
    else:
        raise FileNotFoundError(f"SQL file not found: {sql_file}")


def upgrade() -> None:
    """Upgrade schema."""
    print("🚀 Creating REIA database schema...")

    # Create all SQLAlchemy tables
    # Import all models to ensure they're registered
    from reia.datamodel import (asset, calculations, fragility,  # noqa
                                lossvalues, vulnerability)
    from reia.datamodel.base import ORMBase
    from reia.repositories import engine

    # Create all tables defined by SQLAlchemy models
    ORMBase.metadata.create_all(engine)
    print("SQLAlchemy tables created.")

    print("Applying REIA custom SQL scripts...")
    # materialized view
    execute_sql_file("materialized_loss_buildings.sql")

    # triggers and functions
    execute_sql_file("trigger_refresh_materialized.sql")
    execute_sql_file("trigger_partition_aggregationtags.sql")
    execute_sql_file("trigger_partition_losstype.sql")

    # indexes
    execute_sql_file("indexes.sql")

    print("All REIA custom SQL scripts applied successfully.")
    print("Database schema created successfully with all custom functions!")


def downgrade() -> None:
    """Downgrade schema with proper handling of complex dependencies."""
    print("Removing REIA database schema...")

    # Use the existing database connection
    from reia.repositories import engine

    with engine.connect() as conn:
        # Drop materialized views first (they depend on tables)
        conn.execute(sa.text(
            "DROP MATERIALIZED VIEW IF EXISTS "
            "loss_buildings_per_municipality CASCADE;"))
        print("Dropped materialized views.")

        # Drop custom triggers explicitly
        conn.execute(sa.text("""
            DROP TRIGGER IF EXISTS
            refresh_materialized_loss_buildings_trigger ON loss_asset;
            DROP TRIGGER IF EXISTS
            insert_calculation_trigger ON loss_calculation;
            DROP TRIGGER IF EXISTS
            insert_aggregationtag_trigger ON loss_exposuremodel;
        """))
        print("Dropped custom triggers.")

        # Drop custom functions
        conn.execute(sa.text("""
            DROP FUNCTION IF EXISTS
                             refresh_materialized_loss_buildings() CASCADE;
            DROP FUNCTION IF EXISTS calculation_partition_function() CASCADE;
            DROP FUNCTION IF EXISTS
                             aggregationtag_partition_function() CASCADE;
        """))
        print("Dropped custom functions.")

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
        print("Dropped custom indexes.")

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

    print("Database schema completely removed.")
