from sqlalchemy import create_engine
from sqlalchemy.schema import MetaData
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

from esloss.datamodel import (  # noqa
    Base, AssetCollection, Asset, Site, Municipality, PostalCode, Canton,
    LossModel, LossCalculation, LossConfig,
    VulnerabilityFunction, VulnerabilityModel,
    MeanAssetLoss, SiteLoss, TaxonomyLoss, MunicipalityPCLoss)

from config import get_config


config = get_config()
engine = create_engine(config.DB_CONNECTION_STRING, echo=False, future=False)

session = scoped_session(sessionmaker(autocommit=False,
                                      autoflush=False,
                                      bind=engine))


ORMBase = declarative_base(cls=Base)
ORMBase.query = session.query_property()


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
