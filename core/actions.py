import time
from configparser import ConfigParser

from esloss.datamodel.calculations import Calculation, EStatus
from requests import Response
from sqlalchemy.orm import Session

from core.db import crud
from core.io.create_input import assemble_calculation_input
from core.io.parse_output import parse_aggregated_risk
from core.oqapi import (oqapi_get_calculation_result, oqapi_get_job_status,
                        oqapi_send_calculation)


def dispatch_openquake_calculation(
        job_file: ConfigParser,
        session: Session) -> Response:
    """
    Assemble and dispatch an OQ calculation.

    :param job_file: Config file for OQ job.
    :param session: Database session object.
    :returns: The Response object from the OpenQuake API.
    """

    # create calculation files
    files = assemble_calculation_input(job_file, session)
    response = oqapi_send_calculation(*files)
    response.raise_for_status()
    return response


def monitor_openquake_calculation(job_id: int,
                                  calculation_oid: int,
                                  session: Session) -> None:
    """
    Monitor OQ calculation and update status accordingly.

    :param job_id: ID of the OQ job.
    :param calculation_oid: ID of the Calculation DB row.
    :param session: Database session object.
    """
    while True:
        response = oqapi_get_job_status(job_id)
        response.raise_for_status()

        status = EStatus[response.json()['status'].upper()]
        crud.update_calculation_status(calculation_oid, status, session)

        if status in (EStatus.COMPLETE, EStatus.ABORTED, EStatus.FAILED):
            return

        time.sleep(1)


def save_openquake_results(calculation: Calculation,
                           job_id: int,
                           session: Session) -> None:
    try:
        dstore = oqapi_get_calculation_result(job_id)
        oq_parameter_inputs = dstore['oqparam']

        if oq_parameter_inputs.calculation_mode == 'scenario_risk':
            df = parse_aggregated_risk(dstore)
            crud.create_aggregated_losses(
                df,
                oq_parameter_inputs.aggregate_by[0],
                calculation._oid,
                calculation._assetcollection_oid,
                session)

    except BaseException as e:
        session.rollback()
        crud.update_calculation_status(
            calculation._oid, EStatus.FAILED, session)
        raise e

    return None
