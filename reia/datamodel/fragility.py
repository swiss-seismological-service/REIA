from sqlalchemy.orm import relationship
from sqlalchemy.sql.schema import Column, ForeignKey
from sqlalchemy.sql.sqltypes import BigInteger, Enum, Float, String

from reia.datamodel.base import ORMBase
from reia.datamodel.lossvalues import ELossCategory
from reia.datamodel.mixins import (ClassificationMixin, CompatibleFloatArray,
                                   CompatibleStringArray, CreationInfoMixin,
                                   PublicIdMixin)


class FragilityModel(ORMBase, PublicIdMixin, CreationInfoMixin):
    """
    Fragility Model
    Instance of SQLAlchemy Single Table Inheritance
    """

    name = Column(String)
    description = Column(String)
    assetcategory = Column(String)
    limitstates = Column(CompatibleStringArray)

    fragilityfunctions = relationship(
        'FragilityFunction',
        back_populates='fragilitymodel',
        cascade='all, delete-orphan',
        passive_deletes=True,
        lazy='joined')

    _type = Column(Enum(ELossCategory))

    __mapper_args__ = {
        "polymorphic_on": _type,
        "polymorphic_identity": ELossCategory.NULL,
    }


class ContentsFragilityModel(FragilityModel):
    __tablename__ = None
    __mapper_args__ = {
        "polymorphic_identity": ELossCategory.CONTENTS,
    }


class StructuralFragilityModel(FragilityModel):
    __tablename__ = None
    __mapper_args__ = {
        "polymorphic_identity": ELossCategory.STRUCTURAL,
    }


class NonstructuralFragilityModel(FragilityModel):
    __tablename__ = None
    __mapper_args__ = {
        "polymorphic_identity": ELossCategory.NONSTRUCTURAL,
    }


class BusinessInterruptionFragilityModel(FragilityModel):
    __tablename__ = None
    __mapper_args__ = {
        "polymorphic_identity": ELossCategory.BUSINESS_INTERRUPTION,
    }


class FragilityFunction(ClassificationMixin('taxonomy'), ORMBase):
    """Fragility Function Model"""

    _fragilitymodel_oid = Column(
        BigInteger,
        ForeignKey('loss_fragilitymodel._oid', ondelete='CASCADE'))
    fragilitymodel = relationship(
        'FragilityModel',
        back_populates='fragilityfunctions')

    format = Column(String)
    shape = Column(String)
    nodamagelimit = Column(Float)
    minintensitymeasurelevel = Column(Float)
    maxintensitymeasurelevel = Column(Float)

    intensitymeasuretype = Column(String, nullable=False)
    intensitymeasurelevels = Column(CompatibleFloatArray)

    limitstates = relationship('LimitState',
                               back_populates='fragilityfunction',
                               passive_deletes=True,
                               cascade='all, delete-orphan',
                               lazy='joined')


class LimitState(ORMBase):
    name = Column(String)

    # params
    mean = Column(Float)
    stddev = Column(Float)

    # poes
    poes = Column(CompatibleFloatArray)

    _fragilityfunction_oid = Column(BigInteger, ForeignKey(
        'loss_fragilityfunction._oid', ondelete='CASCADE'))
    fragilityfunction = relationship('FragilityFunction',
                                     back_populates='limitstates')


class TaxonomyMap(ORMBase, CreationInfoMixin):
    name = Column(String)
    mappings = relationship('Mapping',
                            back_populates='taxonomymap',
                            passive_deletes=True,
                            cascade='all, delete-orphan',
                            lazy='joined')


class Mapping(ORMBase):
    taxonomy = Column(String)
    conversion = Column(String)
    weight = Column(Float)

    _taxonomymap_oid = Column(BigInteger, ForeignKey(
        'loss_taxonomymap._oid', ondelete='CASCADE'))
    taxonomymap = relationship('TaxonomyMap',
                               back_populates='mappings')
