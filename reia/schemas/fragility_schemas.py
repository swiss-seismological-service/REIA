from typing import List, Optional

from pydantic import Field

from reia.schemas.base import CreationInfoMixin, Model, TaxonomyMixin
from reia.schemas.vulnerability_schemas import ELossCategory


class LimitState(Model):
    oid: Optional[int] = Field(default=None, alias='_oid')
    name: Optional[str] = None
    mean: Optional[float] = None
    stddev: Optional[float] = None
    poes: Optional[List[float]] = None
    fragilityfunction_oid: Optional[int] = Field(
        default=None, alias='_fragilityfunction_oid')


class FragilityFunction(TaxonomyMixin):
    oid: Optional[int] = Field(default=None, alias='_oid')
    fragilitymodel_oid: Optional[int] = Field(
        default=None, alias='_fragilitymodel_oid')
    format: Optional[str] = None
    shape: Optional[str] = None
    nodamagelimit: Optional[float] = None
    minintensitymeasurelevel: Optional[float] = None
    maxintensitymeasurelevel: Optional[float] = None
    intensitymeasuretype: Optional[str] = None
    intensitymeasurelevels: Optional[list[float]] = None
    limitstates: list[LimitState] = []


class FragilityModel(CreationInfoMixin):
    oid: Optional[int] = Field(default=None, alias='_oid')
    name: Optional[str] = None
    description: Optional[str] = None
    assetcategory: Optional[str] = None
    publicid: Optional[str] = None
    type: Optional[ELossCategory] = Field(default=None, alias='_type')
    limitstates: Optional[list[str]] = None
    fragilityfunctions: List[FragilityFunction] = []


class ContentsFragilityModel(FragilityModel):
    pass


class StructuralFragilityModel(FragilityModel):
    pass


class NonstructuralFragilityModel(FragilityModel):
    pass


class BusinessInterruptionFragilityModel(FragilityModel):
    pass


class Mapping(Model):
    oid: Optional[int] = Field(default=None, alias='_oid')
    taxonomy: Optional[str] = None
    conversion: Optional[str] = None
    weight: Optional[float] = None
    taxonomymap_oid: Optional[int] = Field(
        default=None, alias='_taxonomymap_oid')


class TaxonomyMap(CreationInfoMixin):
    oid: Optional[int] = Field(default=None, alias='_oid')
    name: Optional[str] = None
    mappings: list[Mapping] | None = None
