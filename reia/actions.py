import logging
import time
from configparser import ConfigParser
from typing import Tuple

from openquake.commonlib.datastore import read
from pandas import DataFrame
from requests import Response
from sqlalchemy.orm import Session

from reia.datamodel import CalculationBranch, EStatus
from reia.db import crud, engine
from reia.io import CalculationBranchSettings, ERiskType
from reia.io.dstore import get_risk_from_dstore
from reia.io.read import parse_calculation_input, validate_calculation_input
from reia.io.write import assemble_calculation_input
from reia.oqapi import (oqapi_failed_for_zero_losses,
                        oqapi_get_calculation_result, oqapi_get_job_status,
                        oqapi_send_calculation)

LOGGER = logging.getLogger(__name__)


def create_scenario_calculation(risk_type: ERiskType,
                                aggregation_tags: list,
                                config: dict,
                                session: Session):

    assert sum([loss['weight']
               for loss in config[risk_type.name.lower()]]) == 1

    calculation = crud.create_calculation(
        {'aggregateby': ['Canton;CantonGemeinde'],
         'status': EStatus.CREATED,
         'calculation_mode': risk_type.value,
         'description': config["scenario_name"]},
        session)

    connection = engine.raw_connection()

    try:
        for loss_branch in config[risk_type.name.lower()]:
            branch = crud.create_calculation_branch(
                {'weight': loss_branch['weight'],
                 'status': EStatus.CREATED,
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
            df['_type'] = risk_type.name
            LOGGER.info('Saving risk values to database...')
            crud.create_risk_values(df, aggregation_tags, connection)
            crud.update_calculation_branch_status(
                branch._oid, EStatus.COMPLETE, session)
            LOGGER.info('Successfully saved risk values to database.')

        crud.update_calculation_status(
            calculation._oid, EStatus.COMPLETE, session)

    except Exception as e:
        crud.update_calculation_status(
            calculation._oid, EStatus.FAILED, session)
        if branch:
            crud.update_calculation_branch_status(
                branch._oid, EStatus.FAILED, session)
        connection.close()
        raise e
    connection.close()

    return calculation


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
            if status == EStatus.FAILED and oqapi_failed_for_zero_losses(
                    job_id):
                crud.update_calculation_branch_status(
                    calculation_branch_oid, EStatus.COMPLETE, session)
            return

        time.sleep(1)


def save_openquake_results(calculationbranch: CalculationBranch,
                           job_id: int,
                           session: Session) -> None:

    dstore = oqapi_get_calculation_result(job_id)
    oq_parameter_inputs = dstore['oqparam']

    aggregation_tags = {}
    for type in [it for sub in oq_parameter_inputs.aggregate_by for it in sub]:
        type_tags = crud.read_aggregationtags(type, session)
        aggregation_tags.update({tag.name: tag for tag in type_tags})

    risk_type = ERiskType(oq_parameter_inputs.calculation_mode)

    df = get_risk_from_dstore(dstore, risk_type)

    df['weight'] = df['weight'] * calculationbranch.weight
    df['_calculation_oid'] = calculationbranch._calculation_oid
    df[f'_{risk_type.name.lower()}calculationbranch_oid'] = \
        calculationbranch._oid
    df['_type'] = risk_type.name

    connection = session.get_bind().raw_connection()
    crud.create_risk_values(df, aggregation_tags, connection)
    connection.close()
    return None


def run_openquake_calculations(
        branch_settings: list[CalculationBranchSettings],
        session: Session):

    # validate that required inputs are set and compatible with each other
    validate_calculation_input(branch_settings)

    # parse information to separate dicts
    calculation_dict, branches_dicts = parse_calculation_input(branch_settings)

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

        return calculation

    except BaseException as e:
        session.rollback()
        for el in session.identity_map.values():
            if hasattr(el, 'status') and el.status != EStatus.COMPLETE:
                el.status = EStatus.ABORTED if isinstance(
                    e, KeyboardInterrupt) else EStatus.FAILED
                session.commit()
        raise e


def read_gmfs(dstore: str) -> Tuple[DataFrame, DataFrame]:
    store = read(dstore)

    site_collection = store.read_df('sitecol')[['sids', 'lon', 'lat']]
    gmf_data = store.read_df('gmf_data')

    site_collection.rename(columns={'sids': 'site_id'}, inplace=True)

    return (gmf_data, site_collection)
