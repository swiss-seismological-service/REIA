from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.schema import Column
from sqlalchemy.sql.sqltypes import BigInteger

from reia.config.settings import get_settings


class ORMBase(DeclarativeBase, AsyncAttrs):
    @declared_attr
    def __tablename__(cls):
        return f'loss_{cls.__name__.lower()}'

    _oid = Column(BigInteger, primary_key=True)

    def _asdict(self):
        dict_ = {}
        for key in self.__mapper__.c.keys():
            dict_[key] = getattr(self, key)
        return dict_


def load_engine():
    config = get_settings()
    engine = create_engine(config.db_connection_string,
                           echo=False,
                           future=True)
    return engine
