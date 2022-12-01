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


def oqapi_send_calculation(*args: io.StringIO):
    args = list(args)
    job_config = args.pop(
        next((i for i, f in enumerate(args) if f.name == 'job.ini')))

    files = {f'input_model_{i+1}': v for i, v in enumerate(args)}
    files['job_config'] = job_config

    response = requests.post(
        f'{config.OQ_API_SERVER}/v1/calc/run', files=files)
    return response


def oqapi_get_job_status(job_id: int) -> requests.Response:
    return requests.get(f'{config.OQ_API_SERVER}/v1/calc/{job_id}/status')


def oqapi_get_calculation_result(job_id: int) -> datastore.DataStore:
    dbserver.ensure_on()

    # if id doesn not exist locally, try getting it on remote
    job = logs.dbcmd('get_job', job_id)
    if job is None:
        oqapi_import_remote_calculation(job_id)

    return datastore.read(job_id)


def oqapi_import_remote_calculation(calc_id: int | str):
    """
    Import a remote calculation into the local database.
    NB: calc_id can be a local pathname to a datastore not already
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
        webex = WebExtractor(calc_id, config.OQ_API_SERVER)
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
