from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import BigInteger, Float, Integer, String
from datamodel.mixins import PublicIdMixin
from datamodel.base import ORMBase


class Calculation(PublicIdMixin, ORMBase):
    """Calculation model"""
    m_shakemapid_resourceid = Column(String)
    m_map_id = Column(BigInteger)


class CalculationParameters(ORMBase):
    """Calculation Parameters model"""
    _parent_oid = Column(BigInteger)
    m_lossunit = Column(String)
    m_maincalculationmode = Column(String)
    m_masterseed = Column(Integer)
    m_maximumdistance = Column(Float)
    m_numberofgroundmotionfields = Column(Integer)
    m_preparationcalculationmode = Column(String)
    m_randomseed = Column(Integer)
