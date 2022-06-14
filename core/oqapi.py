import io
import time
import requests


def oqapi_send_calculation(
        job_config: io.StringIO,
        *args: io.StringIO):

    files = {f'input_model_{i+1}': v for i, v in enumerate(args)}
    files['job_config'] = job_config

    response = requests.post(
        'http://localhost:8800/v1/calc/run', files=files)
    if response.ok:
        print(
            'Successfully sent calculation job to OpenQuake.')
        return response
    else:
        print(
            'Error sending the calculation job to OpenQuake.')
        return response


def oqapi_get_job_status(job_id):
    return requests.get(f'http://localhost:8800/v1/calc/{job_id}/status')\
        .json()['status']


def oqapi_wait_for_job(job_id):
    check_status = oqapi_get_job_status(job_id)

    while check_status != 'complete':
        time.sleep(1)
        check_status = oqapi_get_job_status(job_id)
        if check_status == 'failed':
            return Exception(check_status.json())
