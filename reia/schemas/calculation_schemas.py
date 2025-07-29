from __future__ import annotations

import configparser
import uuid
from typing import Any

from pydantic import BaseModel, Field, field_validator

from reia.schemas.base import CreationInfoMixin, Model
from reia.schemas.enums import ECalculationType, EEarthquakeType, EStatus
from reia.schemas.lossvalue_schemas import DamageValue, LossValue


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
    losses: list[LossValue] = Field(default=[], exclude=True)
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
    type: ECalculationType = Field(default=ECalculationType.LOSS,
                                   alias='_type')


class DamageCalculationBranch(CalculationBranch):
    damages: list[DamageValue] = Field(default=[], exclude=True)
    contentsfragilitymodel_oid: int | None = Field(
        default=None, alias='_contentsfragilitymodel_oid')
    structuralfragilitymodel_oid: int | None = Field(
        default=None, alias='_structuralfragilitymodel_oid')
    nonstructuralfragilitymodel_oid: int | None = Field(
        default=None, alias='_nonstructuralfragilitymodel_oid')
    businessinterruptionfragilitymodel_oid: int | None = Field(
        default=None, alias='_businessinterruptionfragilitymodel_oid')
    type: ECalculationType = Field(
        default=ECalculationType.DAMAGE, alias='_type')


class Calculation(CreationInfoMixin):
    oid: int | None = Field(default=None, alias='_oid')
    aggregateby: list[str] = []
    status: EStatus | None = None
    description: str | None = None
    type: ECalculationType | None = Field(default=None, alias='_type')


class LossCalculation(Calculation):
    losses: list[LossValue] = Field(default=[], exclude=True)
    losscalculationbranches: list[LossCalculationBranch] = Field(
        default=[], exclude=True)
    type: ECalculationType = Field(default=ECalculationType.LOSS,
                                   alias='_type')


class DamageCalculation(Calculation):
    damages: list[DamageValue] = Field(default=[], exclude=True)
    damagecalculationbranches: list[DamageCalculationBranch] = Field(
        default=[], exclude=True)
    type: ECalculationType = Field(
        default=ECalculationType.DAMAGE, alias='_type')


class CalculationBranchSettings(Model):
    """ Contains the weight and a OQ settings file for a calculation"""
    weight: float
    config: configparser.ConfigParser


class BranchInputSchema(BaseModel):
    """Schema for validating and parsing individual branch configuration."""
    weight: float
    calculation_mode: str
    description: str | None = None
    aggregate_by: str | None = None
    exposuremodel_oid: int
    config_dict: dict[str, Any]  # Flattened general config
    number_of_ground_motion_fields: int
    vulnerability_models: dict[str, int] | None = None
    fragility_models: dict[str, int] | None = None
    taxonomymap_oid: int | None = None

    @field_validator('weight')
    def validate_weight_range(cls, v):
        """Ensure weight is between 0 and 1"""
        if not 0 < v <= 1:
            raise ValueError('Weight must be between 0 and 1')
        return v
