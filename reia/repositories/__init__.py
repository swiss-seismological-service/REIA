import numpy as np
import pandas as pd
from psycopg2.extensions import AsIs, register_adapter
from sqlalchemy import Select
from sqlalchemy import create_engine as _create_engine
from sqlalchemy.engine import URL, Engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.session import Session
from sqlalchemy.schema import MetaData
from sqlalchemy.sql import text

from reia.config.settings import get_settings
from reia.datamodel.base import ORMBase

EXTENSIONS = []


def create_extensions(engine):
    with engine.connect() as conn:
        for extension in EXTENSIONS:
            conn.execute(
                text(f'CREATE EXTENSION IF NOT EXISTS "{extension}"'))
            conn.commit()


def create_engine(
        url: URL | str,
        skip_extensions: bool = False,
        **kwargs) -> Engine:
    config = get_settings()
    _engine = _create_engine(
        url,
        future=True,
        pool_size=config.postgres_pool_size,
        max_overflow=config.postgres_max_overflow,
        **kwargs,
    )
    if not skip_extensions:
        create_extensions(_engine)
    return _engine


config = get_settings()
engine = create_engine(config.db_connection_string, skip_extensions=True)
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


register_adapter(np.int64, AsIs)


def pandas_read_sql(stmt: Select, session: Session):
    df = pd.read_sql_query(stmt, session.connection())
    return df
