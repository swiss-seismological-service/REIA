import logging
import time
from configparser import ConfigParser

from esloss.datamodel import CalculationBranch, EStatus
from openquake.commonlib.datastore import read
from requests import Response
from sqlalchemy.orm import Session

from core.db import crud, engine
from core.io import CalculationBranchSettings, ERiskType
from core.io.dstore import get_risk_from_dstore
from core.io.read import parse_calculation_input, validate_calculation_input
from core.io.write import assemble_calculation_input
from core.oqapi import (oqapi_get_calculation_result, oqapi_get_job_status,
                        oqapi_send_calculation)

LOGGER = logging.getLogger(__name__)


def create_risk_scenario(earthquake_oid: int,
                         risk_type: ERiskType,
                         aggregation_tags: list,
                         config: dict,
                         session: Session):

    assert sum([loss['weight']
               for loss in config[risk_type.name.lower()]]) == 1

    calculation = crud.create_calculation(
        {'aggregateby': ['Canton;CantonGemeinde'],
         'status': EStatus.COMPLETE,
         '_earthquakeinformation_oid': earthquake_oid,
         'calculation_mode': risk_type.value,
         'description': config["scenario_name"]},
        session)

    connection = engine.raw_connection()

    for loss_branch in config[risk_type.name.lower()]:
        branch = crud.create_calculation_branch(
            {'weight': loss_branch['weight'],
             'status': EStatus.COMPLETE,
             '_calculation_oid': calculation._oid,
             '_exposuremodel_oid': loss_branch['exposure'],
             'calculation_mode': risk_type.value},
            session)
        LOGGER.info(f'Parsing datastore {loss_branch["store"]}')

        dstore_path = f'{config["folder"]}/{loss_branch["store"]}'
        dstore = read(dstore_path)
        df = get_risk_from_dstore(dstore, risk_type)

        df['weight'] = df['weight'] * loss_branch['weight']
        df['_calculation_oid'] = calculation._oid
        df[f'_{risk_type.name.lower()}calculationbranch_oid'] = branch._oid
        df['_type'] = f'{risk_type.name.lower()}value'
        LOGGER.info('Saving risk values to database...')
        crud.create_risk_values(df, aggregation_tags, connection)
        LOGGER.info('Successfully saved risk values to database.')

    connection.close()


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

    aggregation_tags = {}
    for type in oq_parameter_inputs.aggregate_by[0]:
        type_tags = crud.read_aggregationtags(type, session)
        aggregation_tags.update({tag.name: tag for tag in type_tags})

    risk_type = ERiskType(oq_parameter_inputs.calculation_mode)

    df = get_risk_from_dstore(dstore, risk_type)

    df['weight'] = df['weight'] * calculationbranch.weight
    df['_calculation_oid'] = calculationbranch._calculation_oid
    df[f'_{risk_type.name.lower()}calculationbranch_oid'] = \
        calculationbranch._oid
    df['_type'] = f'{risk_type.name.lower()}value'

    connection = session.get_bind().raw_connection()
    crud.create_risk_values(df, aggregation_tags, connection)
    connection.close()
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
