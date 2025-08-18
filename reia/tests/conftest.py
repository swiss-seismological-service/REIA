import pytest

from reia.tests.database import (get_test_session, setup_test_database,
                                 teardown_test_database)


@pytest.fixture(scope='session', autouse=True)
def setup_test_env():
    """Set up test environment with database."""
    setup_test_database()
    yield
    teardown_test_database()


@pytest.fixture(scope='module')
def db_session():
    """Database session fixture that matches production setup."""
    session = get_test_session()
    yield session
    session.remove()
