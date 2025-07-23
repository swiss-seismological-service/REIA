from geoalchemy2 import Geometry
from sqlalchemy import ForeignKeyConstraint, Table, UniqueConstraint
from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import BigInteger, Float, Integer, String

from reia.datamodel.base import ORMBase
from reia.datamodel.lossvalues import riskvalue_aggregationtag
from reia.datamodel.mixins import ClassificationMixin

asset_aggregationtag = Table(
    'loss_assoc_asset_aggregationtag',
    ORMBase.metadata,
    Column('asset', ForeignKey('loss_asset._oid',
                               ondelete='CASCADE')),

    Column('aggregationtag', BigInteger),
    Column('aggregationtype', String),

    ForeignKeyConstraint(['aggregationtag',
                          'aggregationtype'],
                         ['loss_aggregationtag._oid',
                         'loss_aggregationtag.type'],
                         ondelete='CASCADE'),
)


class Asset(ORMBase, ClassificationMixin('taxonomy')):
    '''Asset model'''

    buildingcount = Column(Integer, nullable=False)

    contentsvalue = Column(Float)
    structuralvalue = Column(Float)
    nonstructuralvalue = Column(Float)
    dayoccupancy = Column(Float)
    nightoccupancy = Column(Float)
    transitoccupancy = Column(Float)
    businessinterruptionvalue = Column(Float)

    aggregationtags = relationship('AggregationTag',
                                   secondary=asset_aggregationtag,
                                   back_populates='assets')

    _exposuremodel_oid = Column(BigInteger,
                                ForeignKey('loss_exposuremodel._oid',
                                           ondelete='CASCADE'))
    exposuremodel = relationship('ExposureModel',
                                 back_populates='assets')

    # site relationship
    _site_oid = Column(BigInteger,
                       ForeignKey('loss_site._oid'),
                       nullable=False)
    site = relationship('Site',
                        back_populates='assets',
                        lazy='joined')


class Site(ORMBase):
    '''Site model'''

    longitude = Column(Float, nullable=False)
    latitude = Column(Float, nullable=False)

    # asset collection relationship
    _exposuremodel_oid = Column(BigInteger,
                                ForeignKey('loss_exposuremodel._oid',
                                           ondelete='CASCADE'))
    exposuremodel = relationship('ExposureModel',
                                 back_populates='sites')

    assets = relationship('Asset',
                          back_populates='site')


class AggregationTag(ORMBase):
    _oid = Column(BigInteger,
                  autoincrement=True,
                  primary_key=True)

    type = Column(String, primary_key=True)
    name = Column(String)

    assets = relationship('Asset',
                          secondary=asset_aggregationtag,
                          back_populates='aggregationtags')

    riskvalues = relationship('RiskValue',
                              secondary=riskvalue_aggregationtag,
                              back_populates='aggregationtags'
                              )

    geometries = relationship('AggregationGeometry',
                              back_populates='aggregationtag')

    _exposuremodel_oid = Column(BigInteger,
                                ForeignKey('loss_exposuremodel._oid',
                                           ondelete='CASCADE'))
    exposuremodel = relationship('ExposureModel',
                                 back_populates='aggregationtags')

    __table_args__ = (
        UniqueConstraint('name',
                         'type',
                         '_exposuremodel_oid'),
        {
            'postgresql_partition_by': 'LIST (type)',
        }
    )


class AggregationGeometry(ORMBase):
    """Aggregation Geometry model"""

    name = Column(String)

    _aggregationtag_oid = Column(BigInteger, nullable=True)
    _aggregationtype = Column(String)

    aggregationtag = relationship(
        'AggregationTag',
        back_populates='geometries')

    _exposuremodel_oid = Column(BigInteger,
                                ForeignKey('loss_exposuremodel._oid',
                                           ondelete='CASCADE'))
    exposuremodel = relationship('ExposureModel',
                                 back_populates='aggregationgeometries')

    geometry = Column(Geometry('MULTIPOLYGON', srid=4326))

    __table_args__ = (
        ForeignKeyConstraint(['_aggregationtag_oid',
                              '_aggregationtype'],
                             ['loss_aggregationtag._oid',
                              'loss_aggregationtag.type']),
    )
