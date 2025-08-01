import numpy as np
from psycopg2.extensions import AsIs, register_adapter
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session, sessionmaker

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

register_adapter(np.int64, AsIs)
