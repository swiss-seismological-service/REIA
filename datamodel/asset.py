from sqlalchemy import Column
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import ForeignKey
from sqlalchemy.sql.sqltypes import BigInteger, Integer, String
from sqlalchemy.dialects.postgresql import ARRAY
from datamodel import ORMBase
from datamodel.mixins import (ClassificationMixin, CreationInfoMixin,
                              PublicIdMixin, RealQuantityMixin)


class AssetCollection(ORMBase, PublicIdMixin, CreationInfoMixin):
    """Asset Collection model"""
    name = Column(String, nullable=False)
    category = Column(String)
    taxonomysource = Column(String)
    costtypes = Column(ARRAY(String))
    tagnames = Column(ARRAY(String))
    occupancyperiods = Column(ARRAY(String))

    lossmodels = relationship(
        'LossModel',
        back_populates='assetcollection')
    assets = relationship(
        'Asset',
        back_populates='assetcollection',
        single_parent=True,
        cascade='all, delete, delete-orphan')
    sites = relationship(
        'Site',
        back_populates='assetcollection',
        single_parent=True,
        cascade='all, delete, delete-orphan')


class Asset(RealQuantityMixin('contentvalue'),
            RealQuantityMixin('structuralvalue'),
            RealQuantityMixin('occupancydaytime'),
            ClassificationMixin('taxonomy'),
            ORMBase):
    """Asset model"""
    _assetcollection_oid = Column(
        BigInteger,
        ForeignKey('loss_assetcollection._oid'))
    assetcollection = relationship(
        'AssetCollection',
        back_populates='assets')

    buildingcount = Column(Integer, nullable=False)

    # site relationship
    _site_oid = Column(
        BigInteger,
        ForeignKey('loss_site._oid'),
        nullable=False)
    site = relationship(
        'Site',
        back_populates='assets',
        lazy='joined')

    # postal code relationship
    _postalcode_oid = Column(
        BigInteger,
        ForeignKey('loss_postalcode._oid'))
    postalcode = relationship(
        'PostalCode',
        back_populates='assets',
        lazy='joined')

    # municipality relationship
    _municipality_oid = Column(
        BigInteger,
        ForeignKey('loss_municipality._oid'))
    municipality = relationship(
        'Municipality',
        back_populates='assets',
        lazy='joined')

    @classmethod
    def get_keys(cls):
        return cls.__table__.c.keys()


class Site(PublicIdMixin,
           RealQuantityMixin('latitude'),
           RealQuantityMixin('longitude'),
           ORMBase):
    """Site model"""
    # asset collection relationship
    _assetcollection_oid = Column(
        BigInteger,
        ForeignKey('loss_assetcollection._oid'))
    assetcollection = relationship(
        'AssetCollection',
        back_populates='sites',
        lazy='joined')

    assets = relationship(
        'Asset',
        back_populates='site')


class PostalCode(ORMBase):
    """PC model"""
    name = Column(String(50))

    assets = relationship(
        'Asset',
        back_populates='postalcode')


class Municipality(ORMBase):
    """Municipality Model"""
    name = Column(String(50))

    assets = relationship(
        'Asset',
        back_populates='municipality')

    # canton relationship
    _canton_oid = Column(
        BigInteger,
        ForeignKey('loss_canton._oid'))
    canton = relationship(
        'Canton',
        back_populates='municipalities',
        lazy='joined')


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
