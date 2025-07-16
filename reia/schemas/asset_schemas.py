from typing import List, Optional

from reia.schemas.base import CreationInfoMixin, Model


class CostType(Model):
    _oid: Optional[int] = None
    name: Optional[str] = None
    type: Optional[str] = None
    unit: Optional[str] = None
    _exposuremodel_oid: Optional[int] = None


class ExposureModel(CreationInfoMixin):
    _oid: Optional[int] = None
    name: Optional[str] = None
    category: Optional[str] = None
    description: Optional[str] = None
    aggregationtypes: Optional[List[str]] = None
    dayoccupancy: Optional[bool] = False
    nightoccupancy: Optional[bool] = False
    transitoccupancy: Optional[bool] = False
    publicid: Optional[str] = None
    taxonomy: Optional[str] = None


class Asset(Model):
    _oid: Optional[int] = None
    buildingcount: Optional[int] = None
    contentsvalue: Optional[float] = None
    structuralvalue: Optional[float] = None
    nonstructuralvalue: Optional[float] = None
    dayoccupancy: Optional[float] = None
    nightoccupancy: Optional[float] = None
    transitoccupancy: Optional[float] = None
    businessinterruptionvalue: Optional[float] = None
    taxonomy: Optional[str] = None
    _exposuremodel_oid: Optional[int] = None
    _site_oid: Optional[int] = None


class Site(Model):
    _oid: Optional[int] = None
    longitude: Optional[float] = None
    latitude: Optional[float] = None
    _exposuremodel_oid: Optional[int] = None


class AggregationTag(Model):
    _oid: Optional[int] = None
    type: Optional[str] = None
    name: Optional[str] = None
    _exposuremodel_oid: Optional[int] = None


class AggregationGeometry(Model):
    _oid: Optional[int] = None
    name: Optional[str] = None
    _aggregationtag_oid: Optional[int] = None
    _exposuremodel_oid: Optional[int] = None
