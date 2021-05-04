from sqlalchemy import Column
from sqlalchemy.sql.sqltypes import BigInteger, Integer
from datamodel.base import ORMBase
from datamodel.mixins import (ClassificationMixin, CreationInfoMixin,
                              PublicIdMixin, RealQuantityMixin)


class Asset(PublicIdMixin,
            RealQuantityMixin('m_contentvalue'),
            RealQuantityMixin('m_structuralvalue'),
            RealQuantityMixin('m_occupancydaytime'),
            ClassificationMixin('m_taxonomy'),
            ORMBase):
    """Asset model"""
    _parent_oid = Column(BigInteger)
    m_buildingcount = Column(Integer)


class AssetCollection(ORMBase, PublicIdMixin, CreationInfoMixin):
    """Asset Collection model"""
