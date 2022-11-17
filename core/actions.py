import time
from configparser import ConfigParser

from esloss.datamodel.calculations import CalculationBranch, EStatus
from requests import Response
from sqlalchemy.orm import Session

from core.db import crud
from core.io.create_input import assemble_calculation_input
from core.io.parse_input import (parse_calculation_input,
                                 validate_calculation_input)
from core.io.parse_output import parse_losses
from core.oqapi import (oqapi_get_calculation_result, oqapi_get_job_status,
                        oqapi_send_calculation)
from core.utils import CalculationBranchSettings


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
                                  calculation_branch_oid: int,
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
        crud.update_calculation_branch_status(
            calculation_branch_oid, status, session)

        if status in (EStatus.COMPLETE, EStatus.ABORTED, EStatus.FAILED):
            return

        time.sleep(1)


def save_openquake_results(calculationbranch: CalculationBranch,
                           job_id: int,
                           session: Session) -> None:

    dstore = oqapi_get_calculation_result(job_id)
    oq_parameter_inputs = dstore['oqparam']

    if oq_parameter_inputs.calculation_mode == 'scenario_risk':
        df = parse_losses(dstore)
        crud.create_losses(
            df,
            oq_parameter_inputs.aggregate_by[0],
            calculationbranch._calculation_oid,
            calculationbranch._oid,
            calculationbranch.weight,
            session)

    return None


def run_openquake_calculations(
        branch_settings: list[CalculationBranchSettings],
        earthquake_oid: int,
        session: Session):

    # validate that required inputs are set and compatible with each other
    validate_calculation_input(branch_settings)

    # parse information to separate dicts
    calculation_dict, branches_dicts = parse_calculation_input(branch_settings)
    calculation_dict['_earthquakeinformation_oid'] = earthquake_oid

    # create the calculation and the branches on the db
    calculation = crud.create_calculation(calculation_dict, session)
    branches = [crud.create_calculation_branch(
        b, session,
        calculation._oid) for b in branches_dicts]

    try:
        crud.update_calculation_status(
            calculation._oid, EStatus.EXECUTING, session)

        for branch in zip(branch_settings, branches):
            # send calculation to OQ and keep updating its status
            response = dispatch_openquake_calculation(
                branch[0].config, session)
            job_id = response.json()['job_id']
            monitor_openquake_calculation(job_id, branch[1]._oid, session)

            print('Calculation finished with status '
                  f'"{EStatus(branch[1].status)}".')

            # Collect OQ results and save to database
            if branch[1].status == EStatus.COMPLETE:
                print('Saving results for calculation branch '
                      f'{branch[1]._oid} with weight {branch[1].weight}')
                save_openquake_results(branch[1], job_id, session)

        status = EStatus.COMPLETE if all(
            b.status == EStatus.COMPLETE for b in branches) else EStatus.FAILED

        crud.update_calculation_status(calculation._oid, status, session)

    except BaseException as e:
        session.rollback()
        for el in session.identity_map.values():
            if hasattr(el, 'status') and el.status != EStatus.COMPLETE:
                el.status = EStatus.ABORTED if isinstance(
                    e, KeyboardInterrupt) else EStatus.FAILED
                session.commit()
        raise e
