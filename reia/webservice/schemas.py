import enum
from datetime import datetime
from typing import Generic, List, TypeVar

from pydantic import BaseModel, ConfigDict, Field, computed_field

from reia.schemas.base import CreationInfoMixin
from reia.schemas.base import Model as CoreModel
from reia.schemas.calculation_schemas import (Calculation, CalculationBranch,
                                              DamageCalculationBranch,
                                              LossCalculationBranch,
                                              RiskAssessment)


class WSModel(CoreModel):
    """Webservice-specific model with relaxed validation for API responses."""
    model_config = ConfigDict(
        extra='allow',  # Allow extra fields for flexibility
        arbitrary_types_allowed=True,
        from_attributes=True,
        protected_namespaces=(),
        populate_by_name=True,
        serialize_by_alias=True
    )


M = TypeVar('M')


class PaginatedResponse(BaseModel, Generic[M]):
    count: int = Field(description='Number of items returned in the response')
    items: List[M] = Field(
        description='List of items returned in the '
        'response following given criteria')


class WSCreationInfoSchema(WSModel):
    """Extended creation info with additional webservice fields."""
    author: str | None = None
    agencyid: str | None = None
    creationtime: datetime | None = None
    version: str | None = None
    copyrightowner: str | None = None
    licence: str | None = None


def creationinfo_factory(obj: WSModel) -> WSCreationInfoSchema:
    return WSCreationInfoSchema(
        author=obj.creationinfo_author,
        agencyid=obj.creationinfo_agencyid,
        creationtime=obj.creationinfo_creationtime,
        version=obj.creationinfo_version,
        copyrightowner=obj.creationinfo_copyrightowner,
        licence=obj.creationinfo_licence)


class WSCreationInfoMixin(CreationInfoMixin, WSModel):
    """Webservice creation info mixin with additional fields."""
    creationinfo_copyrightowner: str | None = Field(default=None, exclude=True)
    creationinfo_licence: str | None = Field(default=None, exclude=True)

    @computed_field
    @property
    def creationinfo(self) -> WSCreationInfoSchema:
        return creationinfo_factory(self)


class ReturnFormats(str, enum.Enum):
    JSON = 'json'
    CSV = 'csv'


class WSAggregationTag(WSModel):
    type: str
    name: str


class WSCalculationBranch(CalculationBranch, WSModel):
    """Webservice version of CalculationBranch"""
    # Add calculation_oid for API (mapped from _calculation_oid in DB)
    calculation_oid: int | None = Field(default=None, alias='_calculation_oid')


class WSLossCalculationBranch(LossCalculationBranch, WSModel):
    """Webservice version of LossCalculationBranch."""
    calculation_oid: int | None = Field(default=None, alias='_calculation_oid')


class WSDamageCalculationBranch(DamageCalculationBranch, WSModel):
    """Webservice version of DamageCalculationBranch."""
    calculation_oid: int | None = Field(default=None, alias='_calculation_oid')


class WSCalculation(Calculation, WSCreationInfoMixin):
    """Webservice version of Calculation - inherits all core fields."""
    pass


class WSLossCalculation(WSCalculation):
    """Loss calculation for webservice API."""
    losscalculationbranches: list[WSLossCalculationBranch] = Field(default=[])
    type: str = Field(default='loss', alias='_type')


class WSDamageCalculation(WSCalculation):
    """Damage calculation for webservice API."""
    damagecalculationbranches: list[WSDamageCalculationBranch] = \
        Field(default=[])
    type: str = Field(default='damage', alias='_type')


class WSRiskAssessment(RiskAssessment, WSCreationInfoMixin):
    """Webservice version of RiskAssessment with navigation properties."""
    # Add navigation properties for API
    losscalculation: WSCalculation | None = None
    damagecalculation: WSCalculation | None = None

    # Hide internal foreign key fields
    losscalculation_oid: None = Field(default=None, exclude=True)
    damagecalculation_oid: None = Field(default=None, exclude=True)


class WSRiskCategory(str, enum.Enum):
    """Webservice-specific risk category naming for API compatibility."""
    CONTENTS = 'contents'
    BUSINESS_INTERRUPTION = 'displaced'
    NONSTRUCTURAL = 'injured'
    OCCUPANTS = 'fatalities'
    STRUCTURAL = 'structural'


class WSRiskValue(WSModel):
    category: WSRiskCategory
    tag: list[str]


class WSLossValueStatistics(WSRiskValue):
    loss_mean: float
    loss_pc10: float
    loss_pc90: float


class WSDamageValueStatistics(WSRiskValue):
    dg1_mean: float
    dg1_pc10: float
    dg1_pc90: float

    dg2_mean: float
    dg2_pc10: float
    dg2_pc90: float

    dg3_mean: float
    dg3_pc10: float
    dg3_pc90: float

    dg4_mean: float
    dg4_pc10: float
    dg4_pc90: float

    dg5_mean: float
    dg5_pc10: float
    dg5_pc90: float

    buildings: float
