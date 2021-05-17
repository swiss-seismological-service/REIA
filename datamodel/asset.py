from sqlalchemy import Column
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.sql.sqltypes import BigInteger, Integer, String
from datamodel.base import ORMBase
from datamodel.mixins import (ClassificationMixin, CreationInfoMixin,
                              PublicIdMixin, RealQuantityMixin)


class AssetCollection(ORMBase, PublicIdMixin, CreationInfoMixin):
    """Asset Collection model"""
    m_lossModels = relationship(
        'LossModel',
        back_populates='m_assetCollection')
    m_assets = relationship(
        'Asset',
        back_populates='m_assetCollection',
        single_parent=True,
        cascade='all, delete, delete-orphan')
    m_sites = relationship(
        'Site',
        back_populates='m_assetCollection',
        single_parent=True,
        cascade='all, delete, delete-orphan')


class Asset(PublicIdMixin,
            RealQuantityMixin('m_contentValue'),
            RealQuantityMixin('m_structuralValue'),
            RealQuantityMixin('m_occupancyDaytime'),
            ClassificationMixin('m_taxonomy'),
            ORMBase):
    """Asset model"""
    _assetCollection_oid = Column(
        BigInteger,
        ForeignKey('loss_assetcollection._oid'))
    m_assetCollection = relationship(
        'AssetCollection',
        back_populates='m_assets',
        lazy='joined')

    m_buildingCount = Column(Integer, nullable=False)

    _site_oid = Column(
        BigInteger,
        ForeignKey('loss_site._oid'),
        nullable=False)
    m_site = relationship(
        'Site',
        back_populates='m_assets',
        lazy='joined')
    _postalCode_oid = Column(
        BigInteger,
        ForeignKey('loss_postalcode._oid'))
    m_postalCode = relationship(
        'PostalCode',
        back_populates='m_assets',
        lazy='joined')


class Site(PublicIdMixin,
           RealQuantityMixin('m_latitude'),
           RealQuantityMixin('m_longitude'),
           ORMBase):
    """Site model"""
    _assetCollection_oid = Column(
        BigInteger,
        ForeignKey('loss_assetcollection._oid'))
    m_assetCollection = relationship(
        'AssetCollection',
        back_populates='m_sites',
        lazy='joined')
    m_assets = relationship(
        'Asset',
        back_populates='m_site')


class PostalCode(ORMBase):
    """PC model"""
    m_plz = Column(Integer, nullable=False)
    _municipality_oid = Column(
        BigInteger,
        ForeignKey('loss_municipality._oid'),
        nullable=False)
    m_municipality = relationship(
        'Municipality',
        back_populates='m_postalCodes',
        lazy='joined')
    m_assets = relationship(
        'Asset',
        back_populates='m_postalCode')


class Municipality(ORMBase):
    """Municipality Model"""
    m_name = Column(String(50), nullable=False)
    m_municipalityId = Column(Integer, nullable=False)
    _canton_oid = Column(
        BigInteger,
        ForeignKey('loss_canton._oid'),
        nullable=False)
    m_canton = relationship(
        'Canton',
        back_populates='m_municipalities',
        lazy='joined')
    m_postalCodes = relationship(
        'PostalCode',
        back_populates='m_municipality',
        single_parent=True,
        cascade='all, delete, delete-orphan')


class Canton(ORMBase):
    """Canton Model"""
    m_name = Column(String(30), nullable=False)
    m_code = Column(String(2), nullable=False)
    m_municipalities = relationship(
        'Municipality',
        back_populates='m_canton',
        single_parent=True,
        cascade='all, delete, delete-orphan'
    )
