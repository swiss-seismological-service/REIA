from sqlalchemy import Column
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.sql.sqltypes import BigInteger, Integer, String
from datamodel.base import ORMBase
from datamodel.mixins import (ClassificationMixin, CreationInfoMixin,
                              PublicIdMixin, RealQuantityMixin)


class AssetCollection(ORMBase, PublicIdMixin, CreationInfoMixin):
    """Asset Collection model"""
    name = Column(String, nullable=False)
    category = Column(String)
    taxonomySource = Column(String)
    lossModels = relationship(
        'LossModel',
        back_populates='assetCollection')
    assets = relationship(
        'Asset',
        back_populates='assetCollection',
        single_parent=True,
        cascade='all, delete, delete-orphan')
    sites = relationship(
        'Site',
        back_populates='assetCollection',
        single_parent=True,
        cascade='all, delete, delete-orphan')


class Asset(PublicIdMixin,
            RealQuantityMixin('contentValue'),
            RealQuantityMixin('structuralValue'),
            RealQuantityMixin('occupancyDaytime'),
            ClassificationMixin('taxonomy'),
            ORMBase):
    """Asset model"""
    _assetCollection_oid = Column(
        BigInteger,
        ForeignKey('loss_assetcollection._oid'))
    assetCollection = relationship(
        'AssetCollection',
        back_populates='assets',
        lazy='joined')

    buildingCount = Column(Integer, nullable=False)

    _site_oid = Column(
        BigInteger,
        ForeignKey('loss_site._oid'),
        nullable=False)
    site = relationship(
        'Site',
        back_populates='assets',
        lazy='joined')
    _postalCode_oid = Column(
        BigInteger,
        ForeignKey('loss_postalcode._oid'))
    postalCode = relationship(
        'PostalCode',
        back_populates='assets',
        lazy='joined')


class Site(PublicIdMixin,
           RealQuantityMixin('latitude'),
           RealQuantityMixin('longitude'),
           ORMBase):
    """Site model"""
    _assetCollection_oid = Column(
        BigInteger,
        ForeignKey('loss_assetcollection._oid'))
    assetCollection = relationship(
        'AssetCollection',
        back_populates='sites',
        lazy='joined')
    assets = relationship(
        'Asset',
        back_populates='site')


class PostalCode(ORMBase):
    """PC model"""
    plz = Column(Integer, nullable=False)
    _municipality_oid = Column(
        BigInteger,
        ForeignKey('loss_municipality._oid'),
        nullable=False)
    municipality = relationship(
        'Municipality',
        back_populates='postalCodes',
        lazy='joined')
    assets = relationship(
        'Asset',
        back_populates='postalCode')


class Municipality(ORMBase):
    """Municipality Model"""
    name = Column(String(50), nullable=False)
    municipalityId = Column(Integer, nullable=False)
    _canton_oid = Column(
        BigInteger,
        ForeignKey('loss_canton._oid'),
        nullable=False)
    canton = relationship(
        'Canton',
        back_populates='municipalities',
        lazy='joined')
    postalCodes = relationship(
        'PostalCode',
        back_populates='municipality',
        single_parent=True,
        cascade='all, delete, delete-orphan')


class Canton(ORMBase):
    """Canton Model"""
    name = Column(String(30), nullable=False)
    code = Column(String(2), nullable=False)
    municipalities = relationship(
        'Municipality',
        back_populates='canton',
        single_parent=True,
        cascade='all, delete, delete-orphan'
    )
