from fastapi import APIRouter, HTTPException, Request

from reia.webservice.database import DBSessionDep
from reia.webservice.repositories import CalculationRepository
from reia.webservice.repositories.aggregation import AggregationRepository
from reia.webservice.schemas import (ReturnFormats, WSLossValueStatistics,
                                     WSRiskCategory)
from reia.webservice.utils import csv_response

router = APIRouter(prefix='/loss', tags=['loss'])


@router.get("/{calculation_id}/{loss_category}/{aggregation_type}",
            response_model=list[WSLossValueStatistics],
            response_model_exclude_none=True)
async def get_losses(calculation_id: int,
                     aggregation_type: str,
                     request: Request,
                     loss_category: WSRiskCategory,
                     db: DBSessionDep,
                     filter_tag_like: str | None = None,
                     format: ReturnFormats = ReturnFormats.JSON,
                     sum: bool = False):
    """
    Returns a list of the loss for a specific category and aggregated
    by a specific aggregation type.
    """

    # Check if calculation exists
    if not await CalculationRepository.get_by_id(db, calculation_id):
        raise HTTPException(
            status_code=404, detail="Loss calculation not found.")

    # Use optimized repository for database-side statistics calculation
    statistics = \
        await AggregationRepository.get_loss_statistics(
            db, calculation_id, aggregation_type,
            loss_category, filter_tag_like)

    if statistics.empty:
        raise HTTPException(
            status_code=404, detail="No data.")

    # Handle sum aggregation if requested
    if sum:
        statistics = statistics.groupby('category').sum().reset_index()
        statistics['tag'] = [[] for _ in range(len(statistics))]

    if format == ReturnFormats.CSV:
        return csv_response('loss', locals())

    return [WSLossValueStatistics.model_validate(x)
            for x in statistics.to_dict('records')]
