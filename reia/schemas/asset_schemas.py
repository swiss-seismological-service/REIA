from typing import Any

from pydantic import Field, field_validator
from shapely import MultiPolygon

from reia.repositories.types import PolygonType, db_to_shapely
from reia.schemas.base import Model, TaxonomyMixin


class AggregationGeometry(Model):
    oid: int | None = Field(default=None, alias='_oid')
    name: str | None = None
    geometry: MultiPolygon | None = None
    aggregationtag_oid: int | None = Field(
        default=None, alias='_aggregationtag_oid')
    exposuremodel_oid: int | None = Field(
        default=None, alias='_exposuremodel_oid')
    aggregationtype: str | None = Field(
        default=None, alias='_aggregationtype')

    @field_validator('geometry', mode='before')
    @classmethod
    def validate_bounding_polygon(cls, value: Any):
        if isinstance(value, PolygonType):
            return db_to_shapely(value)
        return value


class AggregationTag(Model):
    oid: int | None = Field(default=None, alias='_oid')
    type: str | None = None
    name: str | None = None
    geometries: list[AggregationGeometry] = Field(
        default=[], exclude=True)
    exposuremodel_oid: int | None = Field(
        default=None, alias='_exposuremodel_oid')


class Site(Model):
    oid: int | None = Field(default=None, alias='_oid')
    longitude: float | None = None
    latitude: float | None = None
    exposuremodel_oid: int | None = Field(
        default=None, alias='_exposuremodel_oid')


class Asset(TaxonomyMixin):
    oid: int | None = Field(default=None, alias='_oid')
    buildingcount: int | None = None
    contentsvalue: float | None = None
    structuralvalue: float | None = None
    nonstructuralvalue: float | None = None
    dayoccupancy: float | None = None
    nightoccupancy: float | None = None
    transitoccupancy: float | None = None
    businessinterruptionvalue: float | None = None

    exposuremodel_oid: int | None = Field(
        default=None, alias='_exposuremodel_oid')

    site: Site | None = Field(default=None, exclude=True)
    site_oid: int | None = Field(default=None, alias='_site_oid')

    aggregationtags: list[AggregationTag] = Field(
        default=[], exclude=True)
