import enum
import uuid
from typing import List, Optional

from pydantic import Field

from reia.schemas.base import CreationInfoMixin, Model


class EStatus(int, enum.Enum):
    FAILED = 1
    ABORTED = 2
    CREATED = 3
    SUBMITTED = 4
    EXECUTING = 5
    COMPLETE = 6


class EEarthquakeType(str, enum.Enum):
    SCENARIO = 'scenario'
    NATURAL = 'natural'


class ECalculationType(str, enum.Enum):
    RISK = 'risk'
    LOSS = 'loss'
    DAMAGE = 'damage'


class RiskAssessment(CreationInfoMixin):
    oid: Optional[uuid.UUID] = Field(default=None, alias='_oid')
    originid: Optional[str] = None
    status: Optional[EStatus] = None
    type: Optional[EEarthquakeType] = None
    preferred: Optional[bool] = None
    published: Optional[bool] = None
    losscalculation_oid: Optional[int] = Field(
        default=None, alias='_losscalculation_oid')
    damagecalculation_oid: Optional[int] = Field(
        default=None, alias='_damagecalculation_oid')


class CalculationBranch(Model):
    oid: Optional[int] = Field(default=None, alias='_oid')
    config: Optional[dict] = None
    status: Optional[EStatus] = None
    weight: Optional[float] = None
    calculation_oid: Optional[int] = Field(
        default=None, alias='_calculation_oid')
    exposuremodel_oid: Optional[int] = Field(
        default=None, alias='_exposuremodel_oid')
    taxonomymap_oid: Optional[int] = Field(
        default=None, alias='_taxonomymap_oid')
    type: Optional[ECalculationType] = Field(default=None, alias='_type')


class LossCalculationBranch(CalculationBranch):
    occupantsvulnerabilitymodel_oid: Optional[int] = Field(
        default=None, alias='_occupantsvulnerabilitymodel_oid')
    contentsvulnerabilitymodel_oid: Optional[int] = Field(
        default=None, alias='_contentsvulnerabilitymodel_oid')
    structuralvulnerabilitymodel_oid: Optional[int] = Field(
        default=None, alias='_structuralvulnerabilitymodel_oid')
    nonstructuralvulnerabilitymodel_oid: Optional[int] = Field(
        default=None, alias='_nonstructuralvulnerabilitymodel_oid')
    businessinterruptionvulnerabilitymodel_oid: Optional[int] = Field(
        default=None, alias='_businessinterruptionvulnerabilitymodel_oid')


class DamageCalculationBranch(CalculationBranch):
    contentsfragilitymodel_oid: Optional[int] = Field(
        default=None, alias='_contentsfragilitymodel_oid')
    structuralfragilitymodel_oid: Optional[int] = Field(
        default=None, alias='_structuralfragilitymodel_oid')
    nonstructuralfragilitymodel_oid: Optional[int] = Field(
        default=None, alias='_nonstructuralfragilitymodel_oid')
    businessinterruptionfragilitymodel_oid: Optional[int] = Field(
        default=None, alias='_businessinterruptionfragilitymodel_oid')


class Calculation(CreationInfoMixin):
    oid: Optional[int] = Field(default=None, alias='_oid')
    aggregateby: Optional[List[str]] = None
    status: Optional[EStatus] = None
    description: Optional[str] = None
    type: Optional[ECalculationType] = Field(default=None, alias='_type')


class LossCalculation(Calculation):
    pass


class DamageCalculation(Calculation):
    pass
