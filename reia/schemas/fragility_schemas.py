
from pydantic import Field

from reia.schemas.base import CreationInfoMixin, Model, TaxonomyMixin
from reia.schemas.vulnerability_schemas import ELossCategory


class LimitState(Model):
    oid: int | None = Field(default=None, alias='_oid')
    name: str | None = None
    mean: float | None = None
    stddev: float | None = None
    poes: list[float] = []
    fragilityfunction_oid: int | None = Field(
        default=None, alias='_fragilityfunction_oid')


class FragilityFunction(TaxonomyMixin):
    oid: int | None = Field(default=None, alias='_oid')
    fragilitymodel_oid: int | None = Field(
        default=None, alias='_fragilitymodel_oid')
    format: str | None = None
    shape: str | None = None
    nodamagelimit: float | None = None
    minintensitymeasurelevel: float | None = None
    maxintensitymeasurelevel: float | None = None
    intensitymeasuretype: str | None = None
    intensitymeasurelevels: list[float] = []
    limitstates: list[LimitState] = Field(default=[])


class FragilityModel(CreationInfoMixin):
    oid: int | None = Field(default=None, alias='_oid')
    name: str | None = None
    description: str | None = None
    assetcategory: str | None = None
    publicid: str | None = None
    type: ELossCategory | None = Field(default=None, alias='_type')
    limitstates: list[str] = []
    fragilityfunctions: list[FragilityFunction] = Field(
        default=[])


class ContentsFragilityModel(FragilityModel):
    pass


class StructuralFragilityModel(FragilityModel):
    pass


class NonstructuralFragilityModel(FragilityModel):
    pass


class BusinessInterruptionFragilityModel(FragilityModel):
    pass


class Mapping(Model):
    oid: int | None = Field(default=None, alias='_oid')
    taxonomy: str | None = None
    conversion: str | None = None
    weight: float | None = None
    taxonomymap_oid: int | None = Field(
        default=None, alias='_taxonomymap_oid')


class TaxonomyMap(CreationInfoMixin):
    oid: int | None = Field(default=None, alias='_oid')
    name: str | None = None
    mappings: list[Mapping] = Field(default=[])
