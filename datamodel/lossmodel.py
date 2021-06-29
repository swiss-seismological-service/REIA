from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column, ForeignKey, Table
from sqlalchemy.sql.sqltypes import BigInteger, Boolean, Float, Integer, String
from datamodel.mixins import EpochMixin, PublicIdMixin, CreationInfoMixin
from datamodel import ORMBase

loss_vulnerability_association = Table(
    'loss_vulnerability_association', ORMBase.metadata,
    Column('lossmodel_id', BigInteger,
           ForeignKey('loss_lossmodel._oid')),
    Column('vulnerabilitymodel_id', BigInteger,
           ForeignKey('loss_vulnerabilitymodel._oid')))


class LossModel(PublicIdMixin, ORMBase):
    """Calculation model"""
    preparationcalculationmode = Column(String(20), nullable=False)
    maincalculationmode = Column(String(20), nullable=False)
    numberofgroundmotionfields = Column(Integer, nullable=False)
    description = Column(String(100))
    maximumdistance = Column(Integer)
    masterseed = Column(Integer)
    randomseed = Column(Integer)
    truncationlevel = Column(Float)
    spatialcorrelation = Column(Boolean)
    crosscorrelation = Column(Boolean)

    _assetcollection_oid = Column(
        BigInteger,
        ForeignKey('loss_assetcollection._oid'),
        nullable=False)
    assetcollection = relationship(
        'AssetCollection',
        back_populates='lossmodels')
    vulnerabilitymodels = relationship(
        'VulnerabilityModel',
        secondary=loss_vulnerability_association,
        back_populates='lossmodels')
    losscalculations = relationship(
        'LossCalculation',
        back_populates='lossmodel',
        single_parent=True,
        lazy='joined',
        cascade='all, delete, delete-orphan')


class LossCalculation(ORMBase, CreationInfoMixin, EpochMixin('timestamp')):
    """Calculation Parameters model"""
    shakemapid_resourceid = Column(String(100), nullable=False)

    _lossmodel_oid = Column(
        BigInteger,
        ForeignKey('loss_lossmodel._oid'),
        nullable=False)
    lossmodel = relationship(
        'LossModel',
        back_populates='losscalculations',
        lazy='joined')
    losses = relationship(
        'LossValue',
        back_populates='losscalculation',
        single_parent=True,
        cascade='all, delete, delete-orphan')

    losscategory = Column(String(20), nullable=False)
    aggregateBy = Column(String(20))


class LossConfig(ORMBase):
    losscategory = Column(String(20), nullable=False)
    aggregateby = Column(String(20))
    _lossmodel_oid = Column(
        BigInteger,
        ForeignKey('loss_lossmodel._oid'),
        nullable=False)
    lossmodel = relationship(
        'LossModel',
        lazy='joined')
