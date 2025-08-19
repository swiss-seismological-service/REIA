from datetime import datetime

from fastapi import APIRouter, HTTPException, Query, Request

from reia.webservice.database import DBSessionDep, paginate
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
    
    return await paginate(db, query, limit, offset, 
                         model_transformer=CalculationRepository.transform_calculation)


@router.get('/{oid}',
            response_model=WSLossCalculation | WSDamageCalculation,
            response_model_exclude_none=True)
async def read_calculation(oid: int,
                           request: Request,
                           db: DBSessionDep):
    '''
    Returns the requested calculation.
    '''
    db_result = await CalculationRepository.get_by_id(db, oid)

    if not db_result:
        raise HTTPException(status_code=404, detail='No calculation found.')

    return db_result
