
from config import get_config
from app import create_app

import pytest

from sqlalchemy_utils import database_exists, create_database, drop_database


@pytest.fixture(autouse=True, scope='session')
def set_up_database():
    """ create and destroy testing database at start and end of testing session """

    print('Database Set Up')
    url = get_config().DB_CONNECTION_STRING
    if not database_exists(url):
        create_database(url)

    yield

    print('Database Tear Down')
    drop_database(url)


@pytest.fixture(scope='class')
def db_session():
    """ init and drop db tables for each function, yield session and remove it at the end """
    from datamodel import session, drop_db, init_db
    init_db()
    print(f'Database Name: {session.bind.url.database}')
    yield session
    drop_db()


@pytest.fixture(scope='class')
def client(db_session):
    """ create app context and yield a testing client """
    app = create_app()
    with app.test_client() as client:
        with app.app_context():
            yield client
