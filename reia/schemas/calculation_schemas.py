import enum
import uuid
from typing import List, Optional

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
    _oid: Optional[uuid.UUID] = None
    originid: Optional[str] = None
    status: Optional[EStatus] = None
    type: Optional[EEarthquakeType] = None
    preferred: Optional[bool] = None
    published: Optional[bool] = None
    _losscalculation_oid: Optional[int] = None
    _damagecalculation_oid: Optional[int] = None


class CalculationBranch(Model):
    _oid: Optional[int] = None
    config: Optional[dict] = None
    status: Optional[EStatus] = None
    weight: Optional[float] = None
    _calculation_oid: Optional[int] = None
    _exposuremodel_oid: Optional[int] = None
    _taxonomymap_oid: Optional[int] = None
    _type: Optional[ECalculationType] = None


class LossCalculationBranch(CalculationBranch):
    _occupantsvulnerabilitymodel_oid: Optional[int] = None
    _contentsvulnerabilitymodel_oid: Optional[int] = None
    _structuralvulnerabilitymodel_oid: Optional[int] = None
    _nonstructuralvulnerabilitymodel_oid: Optional[int] = None
    _businessinterruptionvulnerabilitymodel_oid: Optional[int] = None


class DamageCalculationBranch(CalculationBranch):
    _contentsfragilitymodel_oid: Optional[int] = None
    _structuralfragilitymodel_oid: Optional[int] = None
    _nonstructuralfragilitymodel_oid: Optional[int] = None
    _businessinterruptionfragilitymodel_oid: Optional[int] = None


class Calculation(CreationInfoMixin):
    _oid: Optional[int] = None
    aggregateby: Optional[List[str]] = None
    status: Optional[EStatus] = None
    description: Optional[str] = None
    _type: Optional[ECalculationType] = None


class LossCalculation(Calculation):
    pass


class DamageCalculation(Calculation):
    pass
