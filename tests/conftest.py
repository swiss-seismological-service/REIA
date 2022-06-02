
from settings import get_config
from app import create_app

import pytest
from contextlib import contextmanager
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


@contextmanager
def _client_impl():
    """ create app context and yield a testing client """
    app = create_app()
    with app.test_client() as client:
        with app.app_context():
            yield client


@contextmanager
def _db_session_impl():
    """ init and drop db tables, yield session and remove it at the end """
    from core.db import session, drop_db, init_db
    init_db()
    print(f'Database Name: {session.bind.url.database}')
    yield session
    drop_db()


@pytest.fixture()
def db_session():
    """ database session scoped to function """
    with _db_session_impl() as session:
        yield session


@pytest.fixture(scope='class')
def db_class():
    """ database session scoped to class """
    with _db_session_impl() as session:
        yield session


@pytest.fixture()
def client(db_session):
    """ client scoped to function """
    with _client_impl() as client:
        yield client


@pytest.fixture(scope='class')
def client_class(db_class):
    """ client scoped to class """
    with _client_impl() as client:
        yield client
