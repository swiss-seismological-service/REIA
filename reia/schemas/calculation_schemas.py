import enum
import uuid
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
    oid: uuid.UUID | None = Field(default=None, alias='_oid')
    originid: str | None = None
    status: EStatus | None = None
    type: EEarthquakeType | None = None
    preferred: bool | None = None
    published: bool | None = None
    losscalculation_oid: int | None = Field(
        default=None, alias='_losscalculation_oid')
    damagecalculation_oid: int | None = Field(
        default=None, alias='_damagecalculation_oid')


class CalculationBranch(Model):
    oid: int | None = Field(default=None, alias='_oid')
    config: dict | None = None
    status: EStatus | None = None
    weight: float | None = None
    calculation_oid: int | None = Field(
        default=None, alias='_calculation_oid')
    exposuremodel_oid: int | None = Field(
        default=None, alias='_exposuremodel_oid')
    taxonomymap_oid: int | None = Field(
        default=None, alias='_taxonomymap_oid')
    type: ECalculationType | None = Field(default=None, alias='_type')


class LossCalculationBranch(CalculationBranch):
    occupantsvulnerabilitymodel_oid: int | None = Field(
        default=None, alias='_occupantsvulnerabilitymodel_oid')
    contentsvulnerabilitymodel_oid: int | None = Field(
        default=None, alias='_contentsvulnerabilitymodel_oid')
    structuralvulnerabilitymodel_oid: int | None = Field(
        default=None, alias='_structuralvulnerabilitymodel_oid')
    nonstructuralvulnerabilitymodel_oid: int | None = Field(
        default=None, alias='_nonstructuralvulnerabilitymodel_oid')
    businessinterruptionvulnerabilitymodel_oid: int | None = Field(
        default=None, alias='_businessinterruptionvulnerabilitymodel_oid')


class DamageCalculationBranch(CalculationBranch):
    contentsfragilitymodel_oid: int | None = Field(
        default=None, alias='_contentsfragilitymodel_oid')
    structuralfragilitymodel_oid: int | None = Field(
        default=None, alias='_structuralfragilitymodel_oid')
    nonstructuralfragilitymodel_oid: int | None = Field(
        default=None, alias='_nonstructuralfragilitymodel_oid')
    businessinterruptionfragilitymodel_oid: int | None = Field(
        default=None, alias='_businessinterruptionfragilitymodel_oid')


class Calculation(CreationInfoMixin):
    oid: int | None = Field(default=None, alias='_oid')
    aggregateby: list[str] | None = None
    status: EStatus | None = None
    description: str | None = None
    type: ECalculationType | None = Field(default=None, alias='_type')


class LossCalculation(Calculation):
    pass


class DamageCalculation(Calculation):
    pass
