from sqlalchemy.sql.schema import Column
from sqlalchemy.sql.sqltypes import Integer, String
from datamodel.mixins import PublicIdMixin, RealQuantityMixin
from datamodel.base import ORMBase


class Site(PublicIdMixin,
           RealQuantityMixin('m_latitude'),
           RealQuantityMixin('m_longitude'),
           ORMBase):
    """Site model"""
    m_canton = Column(String)
    m_municapilityid = Column(Integer)
    m_postalcode = Column(Integer)
