from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column, ForeignKey, Table
from sqlalchemy.sql.sqltypes import BigInteger, Float, Integer, String
from datamodel.mixins import EpochMixin, PublicIdMixin, CreationInfoMixin
from datamodel.base import ORMBase

loss_vulnerability_association = Table(
    'loss_vulnerability_association', ORMBase.metadata,
    Column('lossmodel_id', BigInteger,
           ForeignKey('loss_lossmodel._oid')),
    Column('vulnerabilitymodel_id', BigInteger,
           ForeignKey('loss_vulnerabilitymodel._oid')))


class LossModel(PublicIdMixin, ORMBase):
    """Calculation model"""
    m_shakemapid_resourceid = Column(String(100), nullable=False)
    m_preparationCalculationMode = Column(String(20), nullable=False)
    m_mainCalculationMode = Column(String(20), nullable=False)
    m_numberOfGroundMotionFields = Column(Integer, nullable=False)
    m_maximumDistance = Column(Integer)
    m_masterSeed = Column(Integer)
    m_randomSeed = Column(Integer)
    m_truncationLevel = Column(Float)

    _assetCollection_oid = Column(
        BigInteger,
        ForeignKey('loss_assetcollection._oid'),
        nullable=False)
    m_assetCollection = relationship(
        'AssetCollection',
        back_populates='m_lossModels')
    m_vulnerabilityModels = relationship(
        'VulnerabilityModel',
        secondary=loss_vulnerability_association,
        back_populates='m_lossModels')
    m_lossCalculations = relationship(
        'LossCalculation',
        back_populates='m_lossModel',
        single_parent=True,
        lazy='joined')


class LossCalculation(ORMBase, CreationInfoMixin, EpochMixin('m_timestamp')):
    """Calculation Parameters model"""
    _lossModel_oid = Column(
        BigInteger,
        ForeignKey('loss_lossmodel._oid'),
        nullable=False)
    m_lossModel = relationship(
        'LossModel',
        back_populates='m_lossCalculations',
        lazy='joined')
    m_losses = relationship(
        'LossValue',
        back_populates='m_lossCalculation',
        single_parent=True)

    m_lossCategory = Column(String(20), nullable=False)
    m_aggregateBy = Column(String(20))
