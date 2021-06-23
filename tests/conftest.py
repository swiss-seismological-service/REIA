
from config import get_config
from app import create_app

from unittest import mock
import pytest

import os
from sqlalchemy_utils import database_exists, create_database, drop_database


@pytest.fixture(autouse=True, scope='session')
def mock_settings_env_vars():
    """ set environment variables to testing """
    with mock.patch.dict(os.environ, {'CONFIG_TYPE': 'config.TestingConfig',
                                      'FLASK_ENV': 'development'}):
        yield


@pytest.fixture(autouse=True, scope='session')
def set_up_database(mock_settings_env_vars):
    """ create and destroy testing database at start and end of testing session """

    print('Database Set Up')
    url = get_config().DB_CONNECTION_STRING
    if not database_exists(url):
        create_database(url)

    yield

    print('Database Tear Down')
    drop_database(url)


@pytest.fixture
def db_session():
    """ init and drop db tables for each function, yield session and remove it at the end """
    from datamodel import session, drop_db, init_db
    init_db()
    yield session
    drop_db()


@pytest.fixture
def client(db_session):
    """ create app context and yield a testing client """
    app = create_app()
    with app.test_client() as client:
        with app.app_context():
            yield client
