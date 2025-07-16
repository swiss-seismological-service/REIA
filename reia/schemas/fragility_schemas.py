from typing import List, Optional

from reia.schemas.base import CreationInfoMixin, Model
from reia.schemas.vulnerability_schemas import ELossCategory


class FragilityModel(Model, CreationInfoMixin):
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
    _oid: Optional[int] = None
    _fragilitymodel_oid: Optional[int] = None
    format: Optional[str] = None
    shape: Optional[str] = None
    nodamagelimit: Optional[float] = None
    minintensitymeasurelevel: Optional[float] = None
    maxintensitymeasurelevel: Optional[float] = None
    intensitymeasuretype: Optional[str] = None
    intensitymeasurelevels: Optional[List[float]] = None
    taxonomy: Optional[str] = None


class LimitState(Model):
    _oid: Optional[int] = None
    name: Optional[str] = None
    mean: Optional[float] = None
    stddev: Optional[float] = None
    poes: Optional[List[float]] = None
    _fragilityfunction_oid: Optional[int] = None


class TaxonomyMap(Model, CreationInfoMixin):
    _oid: Optional[int] = None
    name: Optional[str] = None


class Mapping(Model):
    _oid: Optional[int] = None
    taxonomy: Optional[str] = None
    conversion: Optional[str] = None
    _taxonomymap_oid: Optional[int] = None
