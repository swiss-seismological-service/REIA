from sqlalchemy import create_engine, Column, BigInteger, MetaData
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base, declared_attr

from config import Config

engine = create_engine(Config.DB_CONNECTION_STRING,
                       echo=True, future=True)

session = scoped_session(sessionmaker(autocommit=False,
                                      autoflush=False,
                                      bind=engine))


class Base(object):

    @declared_attr
    def __tablename__(cls):
        return f'loss_{cls.__name__.lower()}'

    _oid = Column(BigInteger, primary_key=True)


ORMBase = declarative_base(cls=Base)
ORMBase.query = session.query_property()


def init_db():
    # import all modules here that might define models so that
    # they will be registered properly on the metadata.  Otherwise
    # you will have to import them first before calling init_db()
    ORMBase.metadata.create_all(engine)


def drop_db():
    m = MetaData()
    m.reflect(engine)
    m.drop_all(engine)
