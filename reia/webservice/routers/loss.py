from fastapi import APIRouter, HTTPException, Request

from reia.config.settings import WebserviceSettings
from reia.webservice.database import DBSessionDep
from reia.webservice.repositories import (AggregationRepository,
                                          CalculationRepository)
from reia.webservice.schemas import LossValueStatisticsSchema, ReturnFormats
from reia.webservice.utils import (aggregate_by_branch_and_event,
                                   calculate_statistics, csv_response)

router = APIRouter(prefix='/loss', tags=['loss'])


@router.get("/{calculation_id}/{loss_category}/{aggregation_type}",
            response_model=list[LossValueStatisticsSchema],
            response_model_exclude_none=True)
async def get_losses(calculation_id: int,
                     aggregation_type: str,
                     request: Request,
                     loss_category: WebserviceSettings.RiskCategory,
                     db: DBSessionDep,
                     filter_tag_like: str | None = None,
                     format: ReturnFormats = ReturnFormats.JSON,
                     sum: bool = False):
    """
    Returns a list of the loss for a specific category and aggregated
    by a specific aggregation type.
    """

    like_tag = f'%{filter_tag_like}%' if filter_tag_like else None

    tags = await AggregationRepository.get_aggregation_tags(
        db, aggregation_type, calculation_id, like_tag)

    db_result = await AggregationRepository.get_aggregated_loss(
        db,
        calculation_id,
        aggregation_type,
        loss_category,
        filter_like_tag=like_tag)

    if tags.empty:
        raise HTTPException(
            status_code=404, detail="No aggregationtags found.")

    if db_result.empty:
        if not await CalculationRepository.get_by_id(db, calculation_id):
            raise HTTPException(
                status_code=404, detail="Loss calculation not found.")

    # merge with aggregationtags to add missing (no loss) aggregationtags
    db_result = db_result.merge(
        tags, how='outer', on=aggregation_type) \
        .infer_objects(copy=False).fillna(0)

    if sum:
        db_result = aggregate_by_branch_and_event(db_result, aggregation_type)

    statistics = calculate_statistics(db_result, aggregation_type)

    statistics['category'] = loss_category

    if format == ReturnFormats.CSV:
        return csv_response('loss', locals())

    return [LossValueStatisticsSchema.model_validate(x)
            for x in statistics.to_dict('records')]
