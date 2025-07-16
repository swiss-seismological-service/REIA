from typing import List, Optional

from pydantic import Field

from reia.schemas.base import CreationInfoMixin, Model
from reia.schemas.vulnerability_schemas import ELossCategory


class FragilityModel(CreationInfoMixin):
    _oid: Optional[int] = None
    name: Optional[str] = None
    description: Optional[str] = None
    assetcategory: Optional[str] = None
    limitstates: Optional[List[str]] = None
    publicid: Optional[str] = None
    _type: Optional[ELossCategory] = None


class ContentsFragilityModel(FragilityModel):
    pass


class StructuralFragilityModel(FragilityModel):
    pass


class NonstructuralFragilityModel(FragilityModel):
    pass


class BusinessInterruptionFragilityModel(FragilityModel):
    pass


class FragilityFunction(Model):
    oid: Optional[int] = Field(default=None, alias='_oid')
    fragilitymodel_oid: Optional[int] = Field(
        default=None, alias='_fragilitymodel_oid')
    format: Optional[str] = None
    shape: Optional[str] = None
    nodamagelimit: Optional[float] = None
    minintensitymeasurelevel: Optional[float] = None
    maxintensitymeasurelevel: Optional[float] = None
    intensitymeasuretype: Optional[str] = None
    intensitymeasurelevels: Optional[List[float]] = None
    taxonomy: Optional[str] = None


class LimitState(Model):
    oid: Optional[int] = Field(default=None, alias='_oid')
    name: Optional[str] = None
    mean: Optional[float] = None
    stddev: Optional[float] = None
    poes: Optional[List[float]] = None
    fragilityfunction_oid: Optional[int] = Field(
        default=None, alias='_fragilityfunction_oid')


class TaxonomyMap(CreationInfoMixin):
    oid: Optional[int] = Field(default=None, alias='_oid')
    name: Optional[str] = None


class Mapping(Model):
    oid: Optional[int] = Field(default=None, alias='_oid')
    taxonomy: Optional[str] = None
    conversion: Optional[str] = None
    taxonomymap_oid: Optional[int] = Field(
        default=None, alias='_taxonomymap_oid')
