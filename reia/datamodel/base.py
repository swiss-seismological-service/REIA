import os

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.schema import Column, MetaData
from sqlalchemy.sql.sqltypes import BigInteger


class Base(object):

    @ declared_attr
    def __tablename__(cls):
        return f'loss_{cls.__name__.lower()}'

    _oid = Column(BigInteger, primary_key=True)

    def _asdict(self):
        dict_ = {}
        for key in self.__mapper__.c.keys():
            dict_[key] = getattr(self, key)
        return dict_


ORMBase = declarative_base(cls=Base)


def load_engine():
    load_dotenv(f'{os.getcwd()}/.env')  # load environment variables

    DB_CONNECTION_STRING = \
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:" \
        f"{os.getenv('DB_PASSWORD')}@{os.getenv('POSTGRES_HOST')}" \
        f":{os.getenv('POSTGRES_PORT')}/{os.getenv('DB_NAME')}"

    engine = create_engine(DB_CONNECTION_STRING, echo=False, future=True)
    return engine


def init_db():
    """
    Initializes the Database.
    All DB modules need to be imported when calling this function.
    """
    engine = load_engine()
    ORMBase.metadata.create_all(engine)


def drop_db():
    """Drops all database Tables but leaves the DB itself in place"""

    engine = load_engine()
    m = MetaData()
    m.reflect(engine)

    droptables = [
        t for k,
        t in m.tables.items() if k not in [
            'municipalities',
            'spatial_ref_sys']]

    connection = engine.raw_connection()
    cursor = connection.cursor()

    for table in droptables:
        command = "DROP TABLE IF EXISTS {} CASCADE;".format(table)
        cursor.execute(command)
        connection.commit()

    cursor.close()
