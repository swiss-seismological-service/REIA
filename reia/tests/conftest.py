
import pytest
from sqlalchemy.orm import scoped_session, sessionmaker

from reia.repositories import engine


@pytest.fixture(scope='module')
def db_session():
    session = scoped_session(sessionmaker(autocommit=False,
                                          bind=engine,
                                          future=True))
    yield session
    session.remove()
