from datamodel.base import ORMBase
from datamodel.mixins import RealQuantityMixin


class Asset(RealQuantityMixin('m_contentvalue'), ORMBase):
    """Asset model"""


class AssetCollection(ORMBase):
    """Asset Collection model"""
