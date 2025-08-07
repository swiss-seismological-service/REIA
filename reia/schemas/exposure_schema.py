from pydantic import Field

from reia.schemas.base import CreationInfoMixin, Model, TaxonomyMixin


class CostType(Model):
    oid: int | None = Field(default=None, alias='_oid')
    name: str | None = None
    type: str | None = None
    unit: str | None = None
    exposuremodel_oid: int | None = Field(
        default=None, alias='_exposuremodel_oid')


class ExposureModel(CreationInfoMixin, TaxonomyMixin):
    oid: int | None = Field(default=None, alias='_oid')
    publicid: str | None = None
    name: str | None = None
    category: str | None = None
    description: str | None = None
    aggregationtypes: list[str] = []
    dayoccupancy: bool | None = False
    nightoccupancy: bool | None = False
    transitoccupancy: bool | None = False
    costtypes: list[CostType] = Field([])
