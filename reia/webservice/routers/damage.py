from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Request
from pandas import DataFrame

from reia.webservice.database import DBSessionDep
from reia.webservice.repositories import CalculationRepository
from reia.webservice.repositories.aggregation import \
    AggregationRepositoryOptimized
from reia.webservice.schemas import (ReturnFormats, WSDamageValueStatistics,
                                     WSRiskCategory)
from reia.webservice.utils import csv_response

router = APIRouter(prefix='/damage', tags=['damage'])


async def calculate_damages(
        calculation_id: int,
        aggregation_type: str,
        damage_category: WSRiskCategory,
        db: DBSessionDep,
        filter_tag_like: str | None = None,
        sum: bool = False):

    # Check if calculation exists
    if not await CalculationRepository.get_by_id(db, calculation_id):
        raise HTTPException(
            status_code=404, detail="Damage calculation not found.")

    # Use optimized repository for database-side statistics calculation
    statistics = \
        await AggregationRepositoryOptimized.get_damage_statistics_optimized(
            db, calculation_id, aggregation_type,
            damage_category, filter_tag_like)

    if statistics.empty:
        raise HTTPException(
            status_code=404, detail="No Data.")

    # Handle sum aggregation if requested
    if sum:
        statistics = statistics.groupby('category').sum().reset_index()
        statistics['tag'] = [[] for _ in range(len(statistics))]

    return statistics


@router.get("/{calculation_id}/{damage_category}/{aggregation_type}",
            response_model=list[WSDamageValueStatistics],
            response_model_exclude_none=True)
async def get_damage(
        calculation_id: int,
        damage_category: WSRiskCategory,
        aggregation_type: str,
        request: Request,
        statistics: Annotated[DataFrame, Depends(calculate_damages)],
        filter_tag_like: str | None = None,
        sum: bool = False,
        format: ReturnFormats = ReturnFormats.JSON,):

    if format == ReturnFormats.CSV:
        return csv_response('damage', locals())

    return [WSDamageValueStatistics.model_validate(x)
            for x in statistics.to_dict('records')]
