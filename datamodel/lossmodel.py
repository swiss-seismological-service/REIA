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
    preparationCalculationMode = Column(String(20), nullable=False)
    mainCalculationMode = Column(String(20), nullable=False)
    numberOfGroundMotionFields = Column(Integer, nullable=False)
    description = Column(String(100))
    maximumDistance = Column(Integer)
    masterSeed = Column(Integer)
    randomSeed = Column(Integer)
    truncationLevel = Column(Float)

    _assetCollection_oid = Column(
        BigInteger,
        ForeignKey('loss_assetcollection._oid'),
        nullable=False)
    assetCollection = relationship(
        'AssetCollection',
        back_populates='lossModels')
    vulnerabilityModels = relationship(
        'VulnerabilityModel',
        secondary=loss_vulnerability_association,
        back_populates='lossModels')
    lossCalculations = relationship(
        'LossCalculation',
        back_populates='lossModel',
        single_parent=True,
        lazy='joined',
        cascade='all, delete, delete-orphan')


class LossCalculation(ORMBase, CreationInfoMixin, EpochMixin('timestamp')):
    """Calculation Parameters model"""
    shakemapid_resourceid = Column(String(100), nullable=False)

    _lossModel_oid = Column(
        BigInteger,
        ForeignKey('loss_lossmodel._oid'),
        nullable=False)
    lossModel = relationship(
        'LossModel',
        back_populates='lossCalculations',
        lazy='joined')
    losses = relationship(
        'LossValue',
        back_populates='lossCalculation',
        single_parent=True,
        cascade='all, delete, delete-orphan')

    lossCategory = Column(String(20), nullable=False)
    aggregateBy = Column(String(20))


class LossConfig(ORMBase):
    lossCategory = Column(String(20), nullable=False)
    aggregateBy = Column(String(20))
    _lossModel_oid = Column(
        BigInteger,
        ForeignKey('loss_lossmodel._oid'),
        nullable=False)
    lossModel = relationship(
        'LossModel',
        lazy='joined')
