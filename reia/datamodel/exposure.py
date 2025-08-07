from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import BigInteger, Boolean, String

from reia.datamodel.base import ORMBase
from reia.datamodel.mixins import (ClassificationMixin, CompatibleStringArray,
                                   CreationInfoMixin, PublicIdMixin)


class ExposureModel(ORMBase,
                    PublicIdMixin,
                    CreationInfoMixin,
                    ClassificationMixin('taxonomy')):
    '''Asset Collection model'''
    name = Column(String)
    category = Column(String)
    description = Column(String)
    aggregationtypes = Column(CompatibleStringArray, nullable=False)
    dayoccupancy = Column(Boolean,
                          server_default='false',
                          default=False,
                          nullable=False)
    nightoccupancy = Column(Boolean,
                            server_default='false',
                            default=False,
                            nullable=False)
    transitoccupancy = Column(Boolean,
                              server_default='false',
                              default=False,
                              nullable=False)

    costtypes = relationship('CostType', back_populates='exposuremodel',
                             passive_deletes=True,
                             cascade='all, delete-orphan',
                             lazy='joined')

    calculationbranch = relationship('CalculationBranch',
                                     back_populates='exposuremodel')

    assets = relationship('Asset',
                          back_populates='exposuremodel',
                          passive_deletes=True,
                          cascade='all, delete-orphan',
                          lazy='raise')
    sites = relationship('Site',
                         back_populates='exposuremodel',
                         passive_deletes=True,
                         cascade='all, delete-orphan',
                         lazy='raise')
    aggregationtags = relationship('AggregationTag',
                                   back_populates='exposuremodel',
                                   passive_deletes=True,
                                   cascade='all, delete-orphan',
                                   lazy='raise')
    aggregationgeometries = relationship('AggregationGeometry',
                                         back_populates='exposuremodel',
                                         passive_deletes=True,
                                         cascade='all, delete-orphan',
                                         lazy='raise')


class CostType(ORMBase):
    name = Column(String)
    type = Column(String)
    unit = Column(String)

    _exposuremodel_oid = Column(BigInteger, ForeignKey(
        'loss_exposuremodel._oid', ondelete='CASCADE'))
    exposuremodel = relationship(
        'ExposureModel',
        back_populates='costtypes')
