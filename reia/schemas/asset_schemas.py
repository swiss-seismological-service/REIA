from typing import List, Optional

from pydantic import Field

from reia.schemas.base import CreationInfoMixin, Model, TaxonomyMixin


class CostType(Model):
    oid: Optional[int] = Field(default=None, alias='_oid')
    name: Optional[str] = None
    type: Optional[str] = None
    unit: Optional[str] = None
    exposuremodel_oid: Optional[int] = Field(
        default=None, alias='_exposuremodel_oid')


class ExposureModel(CreationInfoMixin, TaxonomyMixin):
    oid: Optional[int] = Field(default=None, alias='_oid')
    name: Optional[str] = None
    category: Optional[str] = None
    description: Optional[str] = None
    aggregationtypes: Optional[List[str]] = None
    dayoccupancy: Optional[bool] = False
    nightoccupancy: Optional[bool] = False
    transitoccupancy: Optional[bool] = False
    publicid: Optional[str] = None
    costtypes: List[CostType] | list[str] | None = Field(None, exclude=True)


class Asset(Model):
    oid: Optional[int] = Field(default=None, alias='_oid')
    buildingcount: Optional[int] = None
    contentsvalue: Optional[float] = None
    structuralvalue: Optional[float] = None
    nonstructuralvalue: Optional[float] = None
    dayoccupancy: Optional[float] = None
    nightoccupancy: Optional[float] = None
    transitoccupancy: Optional[float] = None
    businessinterruptionvalue: Optional[float] = None
    taxonomy: Optional[str] = None
    exposuremodel_oid: Optional[int] = Field(
        default=None, alias='_exposuremodel_oid')
    site_oid: Optional[int] = Field(default=None, alias='_site_oid')


class Site(Model):
    oid: Optional[int] = Field(default=None, alias='_oid')
    longitude: Optional[float] = None
    latitude: Optional[float] = None
    exposuremodel_oid: Optional[int] = Field(
        default=None, alias='_exposuremodel_oid')


class AggregationTag(Model):
    oid: Optional[int] = Field(default=None, alias='_oid')
    type: Optional[str] = None
    name: Optional[str] = None
    exposuremodel_oid: Optional[int] = Field(
        default=None, alias='_exposuremodel_oid')


class AggregationGeometry(Model):
    oid: Optional[int] = Field(default=None, alias='_oid')
    name: Optional[str] = None
    aggregationtag_oid: Optional[int] = Field(
        default=None, alias='_aggregationtag_oid')
    exposuremodel_oid: Optional[int] = Field(
        default=None, alias='_exposuremodel_oid')
