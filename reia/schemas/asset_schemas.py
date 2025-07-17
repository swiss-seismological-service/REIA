
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
    name: str | None = None
    category: str | None = None
    description: str | None = None
    aggregationtypes: list[str] | None = None
    dayoccupancy: bool | None = False
    nightoccupancy: bool | None = False
    transitoccupancy: bool | None = False
    publicid: str | None = None
    costtypes: list[CostType] | list[str] | None = Field(None, exclude=True)


class Asset(Model):
    oid: int | None = Field(default=None, alias='_oid')
    buildingcount: int | None = None
    contentsvalue: float | None = None
    structuralvalue: float | None = None
    nonstructuralvalue: float | None = None
    dayoccupancy: float | None = None
    nightoccupancy: float | None = None
    transitoccupancy: float | None = None
    businessinterruptionvalue: float | None = None
    taxonomy: str | None = None
    exposuremodel_oid: int | None = Field(
        default=None, alias='_exposuremodel_oid')
    site_oid: int | None = Field(default=None, alias='_site_oid')


class Site(Model):
    oid: int | None = Field(default=None, alias='_oid')
    longitude: float | None = None
    latitude: float | None = None
    exposuremodel_oid: int | None = Field(
        default=None, alias='_exposuremodel_oid')


class AggregationTag(Model):
    oid: int | None = Field(default=None, alias='_oid')
    type: str | None = None
    name: str | None = None
    exposuremodel_oid: int | None = Field(
        default=None, alias='_exposuremodel_oid')


class AggregationGeometry(Model):
    oid: int | None = Field(default=None, alias='_oid')
    name: str | None = None
    aggregationtag_oid: int | None = Field(
        default=None, alias='_aggregationtag_oid')
    exposuremodel_oid: int | None = Field(
        default=None, alias='_exposuremodel_oid')
