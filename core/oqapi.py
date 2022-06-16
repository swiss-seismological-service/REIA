import io
import time
import requests
import pandas as pd
from openquake.commonlib.datastore import read


def oqapi_send_calculation(*args: io.StringIO):
    args = list(args)
    job_config = args.pop(
        next((i for i, f in enumerate(args) if f.name == 'job.ini')))

    files = {f'input_model_{i+1}': v for i, v in enumerate(args)}
    files['job_config'] = job_config

    response = requests.post(
        'http://localhost:8800/v1/calc/run', files=files)
    return response


def oqapi_get_job_status(job_id):
    return requests.get(f'http://localhost:8800/v1/calc/{job_id}/status')\
        .json()['status']


def oqapi_wait_for_job_completion(job_id):
    check_status = oqapi_get_job_status(job_id)

    while check_status not in ('complete', 'failed', 'aborted'):
        time.sleep(1)
        check_status = oqapi_get_job_status(job_id)

    return check_status


def oqapi_wait_for_job_status(job_id: int, status: tuple | str):
    check_status = oqapi_get_job_status(job_id)

    status = status if isinstance(status, tuple) else (status)
    while check_status not in status:
        time.sleep(1)
        check_status = oqapi_get_job_status(job_id)

    return check_status


def oqapi_get_risk_results(job_id: int) -> pd.DataFrame:
    dstore = read(job_id)

    assert(dstore['oqparam'].calculation_mode == 'scenario_risk')

    print([d.decode().split(',') for d in dstore['agg_keys'][:]])
    print(dstore['oqparam'].loss_types)
    print(dstore['oqparam'].aggregate_by[0])
    df = dstore.read_df('risk_by_event')
    print(df)
