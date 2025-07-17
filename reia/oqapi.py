import io
import logging
import pprint
import sys

import requests
from openquake.calculators.extract import WebExtractor
from openquake.commonlib import datastore, logs
from openquake.engine import engine
from openquake.server import dbserver

from settings import get_config

config = get_config()


def oqapi_failed_for_zero_losses(job_id: int) -> bool:
    session = oqapi_auth_session()
    response = session.get(
        f'{config.OQ_API_SERVER}/v1/calc/{job_id}/traceback')
    response.raise_for_status()
    response = response.json()
    empty = 'SystemExit: The risk_by_event table is empty!'
    if any(empty in r for r in response):
        return True
    return False


def oqapi_auth_session() -> requests.Session:
    session = requests.Session()
    session.post(f'{config.OQ_API_SERVER}/accounts/ajax_login/',
                 data=config.OQ_API_AUTH)
    return session


def oqapi_send_calculation(*args: io.StringIO):
    args = list(args)
    job_config = args.pop(
        next((i for i, f in enumerate(args) if f.name == 'job.ini')))

    files = {f'input_model_{i + 1}': v for i, v in enumerate(args)}
    files['job_config'] = job_config
    session = oqapi_auth_session()
    response = session.post(
        f'{config.OQ_API_SERVER}/v1/calc/run', files=files)
    return response


def oqapi_get_job_status(job_id: int) -> requests.Response:
    session = oqapi_auth_session()
    return session.get(f'{config.OQ_API_SERVER}/v1/calc/{job_id}/status')


def oqapi_get_calculation_result(job_id: int) -> datastore.DataStore:
    dbserver.ensure_on()

    # if id doesn not exist locally, try getting it on remote
    job = logs.dbcmd('get_job', job_id)
    if job is None:
        oqapi_import_remote_calculation(job_id)

    return datastore.read(job_id)


def oqapi_import_remote_calculation(calc_id: int | str):
    """Import a remote calculation into the local database.

    Args:
        calc_id: Can be a local pathname to a datastore not already
                present in the database: in that case it is imported in the db.

    Note:
        calc_id can be a local pathname to a datastore not already
        present in the database: in that case it is imported in the db.
    """
    dbserver.ensure_on()
    try:
        calc_id = int(calc_id)
    except ValueError:  # assume calc_id is a pathname
        remote = False
    else:
        remote = True
        job = logs.dbcmd('get_job', calc_id)
        if job is not None:
            sys.exit('There is already a job #%d in the local db' % calc_id)
    if remote:
        datadir = datastore.get_datadir()
        auth = config.OQ_API_AUTH
        webex = WebExtractor(
            calc_id,
            config.OQ_API_SERVER,
            auth['username'],
            auth['password'])
        hc_id = webex.oqparam.hazard_calculation_id
        if hc_id:
            sys.exit('The job has a parent (#%d) and cannot be '
                     'downloaded' % hc_id)
        webex.dump('%s/calc_%d.hdf5' % (datadir, calc_id))
        webex.close()
    with datastore.read(calc_id) as dstore:
        pprint.pprint(dstore.get_attrs('/'))
        engine.expose_outputs(dstore, status='complete')
    logging.info('Imported calculation %s successfully', calc_id)
