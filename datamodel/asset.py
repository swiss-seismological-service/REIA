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
    taxonomySource = Column(String)
    costTypes = Column(ARRAY(String))
    tagNames = Column(ARRAY(String))
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

    def to_dict(self):
        d = {
            'id': self._oid,
            'publicId_resourceId': self.publicId_resourceId,
            'name': self.name,
            'category': self.category,
            'taxonomySource': self.taxonomySource,
            'tagNames': self.tagNames,
            'costTypes': self.costTypes
        }
        return d


class Asset(RealQuantityMixin('contentValue'),
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
        back_populates='assets')

    buildingCount = Column(Integer, nullable=False)

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
    _postalCode_oid = Column(
        BigInteger,
        ForeignKey('loss_postalcode._oid'))
    postalCode = relationship(
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

    def to_dict(self):
        d = {
            'id': self._oid,
            'lon': self.site.longitude_value,
            'lat': self.site.latitude_value,
            'taxonomy': self.taxonomy_concept,
            'number': self.buildingCount,
            'structural': self.structuralvalue_value,
            'contents': self.contentvalue_value,
            'day': self.occupancydaytime_value,
        }
        return d

    @classmethod
    def get_keys(cls):
        return cls.__table__.c.keys()


class Site(PublicIdMixin,
           RealQuantityMixin('latitude'),
           RealQuantityMixin('longitude'),
           ORMBase):
    """Site model"""
    # asset collection relationship
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
    name = Column(String(50))

    assets = relationship(
        'Asset',
        back_populates='postalCode')


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
