import logging
import time
from configparser import ConfigParser
from operator import attrgetter

import pandas as pd
from requests import Response
from sqlalchemy.orm import Session

from reia.datamodel import CalculationBranch, EStatus
from reia.db import crud
from reia.io import CalculationBranchSettings, ERiskType
from reia.io.dstore import get_risk_from_dstore
from reia.io.read import parse_calculation_input, validate_calculation_input
from reia.io.write import assemble_calculation_input
from reia.oqapi import (oqapi_failed_for_zero_losses,
                        oqapi_get_calculation_result, oqapi_get_job_status,
                        oqapi_send_calculation)
from reia.repositories.asset import AggregationTagRepository
from reia.repositories.lossvalue import RiskValueRepository

LOGGER = logging.getLogger(__name__)


def dispatch_openquake_calculation(
        job_file: ConfigParser,
        session: Session) -> Response:
    """Assemble and dispatch an OQ calculation.

    Args:
        job_file: Config file for OQ job.
        session: Database session object.

    Returns:
        The Response object from the OpenQuake API.
    """

    # create calculation files
    files = assemble_calculation_input(job_file, session)
    response = oqapi_send_calculation(*files)
    response.raise_for_status()
    return response


def monitor_openquake_calculation(job_id: int,
                                  calculation_branch_oid: int,
                                  session: Session) -> None:
    """Monitor OQ calculation and update status accordingly.

    Args:
        job_id: ID of the OQ job.
        calculation_branch_oid: ID of the Calculation DB row.
        session: Database session object.
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

    # Get calculation data
    dstore = oqapi_get_calculation_result(job_id)
    oq_parameter_inputs = dstore['oqparam']

    # Flatten and deduplicate types
    all_types = {it for sub in oq_parameter_inputs.aggregate_by for it in sub}

    # Bulk fetch aggregation tags once per exposuremodel
    aggregation_tags_list = AggregationTagRepository.get_by_exposuremodel(
        session, calculationbranch._exposuremodel_oid, types=list(all_types))
    aggregation_tag_by_name = {tag.name: tag for tag in aggregation_tags_list}

    risk_type = ERiskType(oq_parameter_inputs.calculation_mode)

    # Retrieve and enrich risk values
    risk_values = get_risk_from_dstore(dstore, risk_type)
    risk_values = risk_values.copy()  # If risk_values is shared elsewhere

    risk_values['weight'] *= calculationbranch.weight
    risk_values['_calculation_oid'] = calculationbranch._calculation_oid
    risk_values['_calculationbranch_oid'] = calculationbranch._oid
    risk_values['_type'] = risk_type.name
    risk_values['losscategory'] = risk_values['losscategory'].map(
        attrgetter('name'))
    risk_values['_oid'] = pd.RangeIndex(start=1, stop=len(risk_values) + 1)

    # Build many-to-many reference table
    df_agg_val = pd.DataFrame({
        'riskvalue': risk_values['_oid'],
        'aggregationtag': risk_values.pop('aggregationtags'),
        '_calculation_oid': risk_values['_calculation_oid'],
        'losscategory': risk_values['losscategory']
    })

    # Explode aggregation tags (list -> rows)
    df_agg_val = df_agg_val.explode('aggregationtag', ignore_index=True)

    # Map to tag object
    tag_objs = df_agg_val['aggregationtag'].map(aggregation_tag_by_name)
    df_agg_val['aggregationtype'] = tag_objs.map(attrgetter('type'))
    df_agg_val['aggregationtag'] = tag_objs.map(attrgetter('oid'))

    # crud.create_risk_values(risk_values, df_agg_val, session)
    RiskValueRepository.insert_many(
        session, risk_values, df_agg_val)
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
