from pathlib import Path

import psycopg2
from alembic import command
from alembic.config import Config
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy.orm import scoped_session, sessionmaker

from reia.config.settings import TestSettings
from reia.repositories import create_engine


def get_test_engine():
    """Create test database engine."""
    test_config = TestSettings()
    return create_engine(
        test_config.db_connection_string
    )


def get_test_session():
    """Create test database session."""
    engine = get_test_engine()
    return scoped_session(sessionmaker(autocommit=False,
                                       bind=engine,
                                       future=True))


def create_test_database():
    """Create test database if it doesn't exist."""
    test_config = TestSettings()

    # Connect to postgres database to create test database
    conn = psycopg2.connect(
        dbname="postgres",
        user=test_config.postgres_user,
        host=test_config.postgres_host,
        port=test_config.postgres_port,
        password=test_config.postgres_password,
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    try:
        with conn.cursor() as cursor:
            # Check if database exists
            cursor.execute(
                "SELECT 1 FROM pg_database WHERE datname = %s",
                (test_config.db_name,)
            )
            if not cursor.fetchone():
                cursor.execute(f'CREATE DATABASE "{test_config.db_name}"')
    finally:
        conn.close()


def drop_test_database():
    """Drop test database if it exists."""
    test_config = TestSettings()

    # Connect to postgres database to drop test database
    conn = psycopg2.connect(
        dbname="postgres",
        user=test_config.postgres_user,
        host=test_config.postgres_host,
        port=test_config.postgres_port,
        password=test_config.postgres_password,
    )
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)

    try:
        with conn.cursor() as cursor:
            # Try to terminate connections to the test database
            try:
                cursor.execute("""
                    SELECT pg_terminate_backend(pid)
                    FROM pg_stat_activity
                    WHERE datname = %s AND pid <> pg_backend_pid()
                """, (test_config.db_name,))
            except psycopg2.Error:
                # If we can't terminate connections, continue anyway
                pass

            # Drop database if exists
            cursor.execute(f'DROP DATABASE IF EXISTS "{test_config.db_name}"')
    finally:
        conn.close()


def setup_test_database():
    """Set up test database with schema."""
    # Create test database
    create_test_database()

    # Check if migrations are needed and run them
    upgrade_test_database()


def teardown_test_database():
    """Clean up test database."""
    # Downgrade schema
    downgrade_test_database()

    # Drop test database
    drop_test_database()


def upgrade_test_database():
    """Run Alembic upgrade to head on test database."""
    test_config = TestSettings()

    # Configure Alembic for test database
    alembic_cfg = Config()
    alembic_cfg.set_main_option("script_location", str(
        Path(__file__).parent.parent.parent / "alembic"))
    alembic_cfg.set_main_option("sqlalchemy.url",
                                test_config.db_connection_string)

    # create_engine needs to be called to initialize extensions
    get_test_engine()

    # run the upgrade
    command.upgrade(alembic_cfg, "head")


def downgrade_test_database():
    """Run Alembic downgrade to base on test database."""
    test_config = TestSettings()

    # Configure Alembic for test database
    alembic_cfg = Config()
    alembic_cfg.set_main_option("script_location", str(
        Path(__file__).parent.parent.parent / "alembic"))
    alembic_cfg.set_main_option("sqlalchemy.url",
                                test_config.db_connection_string)

    # run the downgrade
    command.downgrade(alembic_cfg, "base")
