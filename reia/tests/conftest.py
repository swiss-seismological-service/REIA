
from unittest.mock import patch

import pytest

from reia.config.settings import TestSettings
from reia.tests.database import (get_test_engine, get_test_session,
                                 setup_test_database, teardown_test_database)


@pytest.fixture(scope='session', autouse=True)
def setup_test_env():
    """Set up test environment with database and patches."""
    # Create test settings instance
    test_settings = TestSettings()

    # Set up test database
    setup_test_database()

    # Patch get_settings to return test settings
    with patch('reia.config.settings.get_settings',
               return_value=test_settings), \
            patch('reia.config.get_settings',
                  return_value=test_settings), \
            patch('reia.repositories.get_settings',
                  return_value=test_settings), \
            patch('reia.repositories.utils.get_settings',
                  return_value=test_settings), \
            patch('reia.repositories.engine',
                  get_test_engine()), \
            patch('reia.repositories.config',
                  test_settings), \
            patch('reia.repositories.DatabaseSession',
                  get_test_session), \
            patch('reia.repositories.utils.make_connection') as mock_conn:

        # Patch make_connection to use test database
        def test_make_connection():
            import psycopg2
            return psycopg2.connect(
                dbname=test_settings.db_name,
                user=test_settings.postgres_user,
                host=test_settings.postgres_host,
                port=test_settings.postgres_port,
                password=test_settings.postgres_password,
            )
        mock_conn.side_effect = test_make_connection

        yield

    # Skip cleanup - let the test database persist for subsequent test runs
    teardown_test_database()


@pytest.fixture(scope='module')
def db_session():
    """Database session fixture that matches production setup."""
    session = get_test_session()
    yield session
    session.remove()
