from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import BigInteger, Float, Integer, String
from datamodel.mixins import RealQuantityMixin
from datamodel.base import ORMBase


class LossByAsset(RealQuantityMixin('m_meanloss'), ORMBase):
    """Loss by asset model"""
    _parent_oid = Column(BigInteger)
    m_assetid = Column(String)


class LossByRealization(ORMBase):
    """Loss by realization model"""
    _parent_oid = Column(BigInteger)
    m_eventrealizationid = Column(Integer)
    m_aggregationidentifier = Column(String)
    m_loss = Column(Float)
