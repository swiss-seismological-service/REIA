from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import BigInteger, Integer, String
from datamodel.mixins import RealQuantityMixin, ClassificationMixin
from datamodel.base import ORMBase


class LossValue(ORMBase, RealQuantityMixin('loss')):
    """
        .. note::

        Inheritance is implemented following the `SQLAlchemy Joined Table
        Inheritance
        <https://docs.sqlalchemy.org/en/latest/orm/inheritance.html#joined-table-inheritance>`_
        paradigm.
    """
    _lossCalculation_oid = Column(
        BigInteger,
        ForeignKey('loss_losscalculation._oid'),
        nullable=False)
    lossCalculation = relationship(
        'LossCalculation',
        back_populates='losses')

    _type = Column(String(25))

    __mapper_args__ = {
        'polymorphic_identity': 'lossvalue',
        'polymorphic_on': _type,
    }


class MeanAssetLoss(LossValue):
    """Loss by asset"""
    __tablename__ = 'loss_meanassetloss'
    _oid = Column(BigInteger, ForeignKey(
        'loss_lossvalue._oid'), primary_key=True)
    _asset_oid = Column(
        BigInteger,
        ForeignKey('loss_asset._oid'),
        nullable=False)
    asset = relationship('Asset')

    __mapper_args__ = {
        'polymorphic_identity': 'meanassetloss'
    }


class SiteLoss(LossValue):
    """Loss by site"""
    __tablename__ = 'loss_siteloss'
    _oid = Column(BigInteger, ForeignKey(
        'loss_lossvalue._oid'), primary_key=True)
    realizationId = Column(Integer, nullable=False)
    _site_oid = Column(
        BigInteger,
        ForeignKey('loss_site._oid'),
        nullable=False
    )
    site = relationship('Site')

    __mapper_args__ = {
        'polymorphic_identity': 'siteloss'
    }


# class PostalCodeLoss(LossValue):
#     """Loss in a postal code area"""
#     __tablename__ = 'loss_postalcodeloss'
#     _oid = Column(BigInteger, ForeignKey(
#         'loss_lossvalue._oid'), primary_key=True)
#     realizationId = Column(Integer, nullable=False)
#     _postalCode_oid = Column(
#         BigInteger,
#         ForeignKey('loss_postalcode._oid'),
#         nullable=False
#     )
#     postalCode = relationship('PostalCode')

#     __mapper_args__ = {
#         'polymorphic_identity': 'postalcodeloss'
#     }

class MunicipalityLoss(LossValue):
    """Loss in a Municipality"""
    __tablename__ = 'loss_municipalityloss'
    _oid = Column(BigInteger, ForeignKey(
        'loss_lossvalue._oid'), primary_key=True)
    realizationId = Column(Integer, nullable=False)
    _municipality_oid = Column(
        BigInteger,
        ForeignKey('loss_municipality._oid'),
        nullable=False
    )
    municipality = relationship('Municipality')

    __mapper_args__ = {
        'polymorphic_identity': 'municipalityloss'
    }


class TaxonomyLoss(LossValue, ClassificationMixin('taxonomy'),):
    """Loss by asset model"""
    __tablename__ = 'loss_taxonomyloss'
    _oid = Column(BigInteger, ForeignKey(
        'loss_lossvalue._oid'), primary_key=True)
    realizationId = Column(Integer, nullable=False)

    __mapper_args__ = {
        'polymorphic_identity': 'taxonomyloss'
    }
