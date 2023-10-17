import numpy as np
from psycopg2.extensions import AsIs, register_adapter
from sqlalchemy import create_engine, create_mock_engine
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.schema import MetaData

from reia.datamodel.base import ORMBase
from settings import get_config

try:
    config = get_config()
    engine = create_engine(
        config.DB_CONNECTION_STRING,
        echo=False,
        future=True)

    session = scoped_session(sessionmaker(autocommit=False,
                                          bind=engine,
                                          future=True))

    ORMBase.query = session.query_property()
except BaseException:
    session = None
    engine = None


def init_db():
    """
    Initializes the Database.
    All DB modules need to be imported when calling this function.
    """
    ORMBase.metadata.create_all(engine)


def drop_db():
    """Drops all database Tables but leaves the DB itself in place"""
    m = MetaData()
    m.reflect(engine)
    m.drop_all(engine)


def init_db_file():
    def dump(sql, *multiparams, **params):
        with open('create_database.sql', 'a') as f:
            f.write(str(sql.compile(dialect=mock_engine.dialect)) + ';')

    mock_engine = create_mock_engine(
        'postgresql+psycopg2://', dump)
    ORMBase.metadata.create_all(bind=mock_engine)


register_adapter(np.int64, AsIs)
