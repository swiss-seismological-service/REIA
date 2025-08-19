from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Request
from sqlalchemy import func, select

from reia.schemas.enums import ECalculationType
from reia.webservice.database import DBSessionDep
from reia.webservice.repositories import CalculationRepository
from reia.webservice.schemas import (PaginatedResponse, WSDamageCalculation,
                                     WSLossCalculation)

router = APIRouter(prefix='/calculation', tags=['calculations'])


@router.get('',
            response_model=PaginatedResponse[WSLossCalculation
                                             | WSDamageCalculation],
            response_model_exclude_none=True)
async def read_calculations(request: Request,
                            db: DBSessionDep,
                            starttime: datetime | None = None,
                            endtime: datetime | None = None,
                            limit: int = Query(50, ge=0),
                            offset: int = Query(0, ge=0)):
    '''
    Returns a list of calculations.
    '''
    query = CalculationRepository.get_filtered_query(starttime, endtime)

    # Get count
    count = await db.scalar(select(func.count()).select_from(query.subquery()))

    # Get items and convert to proper schema based on type
    items = []
    for calc in await db.scalars(query.limit(limit).offset(offset)):
        if calc._type == ECalculationType.LOSS:
            items.append(WSLossCalculation.model_validate(calc))
        elif calc._type == ECalculationType.DAMAGE:
            items.append(WSDamageCalculation.model_validate(calc))

    return {'count': count, 'items': items}


@router.get('/{oid}',
            response_model=WSLossCalculation | WSDamageCalculation,
            response_model_exclude_none=True)
async def read_calculation(oid: int,
                           request: Request,
                           db: DBSessionDep):
    '''
    Returns the requested calculation.
    '''
    # Get the raw calculation from DB
    calc = await CalculationRepository.get_by_id(db, oid)

    if not calc:
        raise HTTPException(status_code=404, detail='No calculation found.')

    # Convert to proper schema based on type
    if hasattr(calc, '_type'):
        if calc._type == ECalculationType.LOSS:
            return WSLossCalculation.model_validate(calc)
        elif calc._type == ECalculationType.DAMAGE:
            return WSDamageCalculation.model_validate(calc)

    return calc
