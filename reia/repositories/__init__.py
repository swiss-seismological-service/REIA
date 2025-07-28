import numpy as np
import pandas as pd
from psycopg2.extensions import AsIs, register_adapter
from sqlalchemy import Select
from sqlalchemy import create_engine as _create_engine
from sqlalchemy import create_mock_engine
from sqlalchemy.engine import URL, Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy.schema import MetaData
from sqlalchemy.sql import text

from reia.datamodel.base import ORMBase
from settings import get_config

config = get_config()
EXTENSIONS = []


def create_extensions(engine):
    with engine.connect() as conn:
        for extension in EXTENSIONS:
            conn.execute(
                text(f'CREATE EXTENSION IF NOT EXISTS "{extension}"'))
            conn.commit()


def create_engine(url: URL | str, **kwargs) -> Engine:
    _engine = _create_engine(
        url,
        future=True,
        pool_size=config.POSTGRES_POOL_SIZE,
        max_overflow=config.POSTGRES_MAX_OVERFLOW,
        **kwargs,
    )
    create_extensions(_engine)
    return _engine


engine = create_engine(config.DB_CONNECTION_STRING)
DatabaseSession = sessionmaker(engine, expire_on_commit=True)


def init_db():
    """Initializes the Database.

    All DB modules need to be imported when calling this function.
    """
    ORMBase.metadata.create_all(engine)


def drop_db():
    """Drops all database Tables but leaves the DB itself in place."""
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


def pandas_read_sql(stmt: Select, session: Session):
    df = pd.read_sql_query(stmt, session.connection())
    return df
