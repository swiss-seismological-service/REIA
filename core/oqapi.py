import io

import pandas as pd
import requests
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


def oqapi_get_job_status(job_id: int) -> requests.Response:
    return requests.get(f'http://localhost:8800/v1/calc/{job_id}/status')


def oqapi_get_risk_results(job_id: int) -> pd.DataFrame:
    dstore = read(job_id)

    assert(dstore['oqparam'].calculation_mode == 'scenario_risk')

    print([d.decode().split(',') for d in dstore['agg_keys'][:]])
    print(dstore['oqparam'].loss_types)
    print(dstore['oqparam'].aggregate_by[0])
    df = dstore.read_df('risk_by_event')
    print(df)
