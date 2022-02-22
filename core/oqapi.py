import io
import time

from flask.helpers import make_response
from flask.json import jsonify
from werkzeug.exceptions import abort

import requests


def oqapi_send_pre_calculation(
        job_config: io.StringIO,
        input_model_1: io.StringIO,
        input_model_2: io.StringIO,
        input_model_3: io.StringIO):
    try:
        response = requests.post(
            'http://localhost:8800/v1/calc/run', files=locals())
        if response.ok:
            print(
                'Successfully sent calculation job to OpenQuake.')
            return response
        else:
            print(
                'Error sending the calculation job to OpenQuake.')
            return response
    except requests.exceptions.ConnectionError:
        print('Could not connect to OpenQuake')
        return abort(make_response(
            jsonify({'error': 'Could not connect to the OpenQuake API'}), 400))


def oqapi_send_main_calculation(job_id, job_config, input_model_1):
    try:
        response = requests.post(
            'http://localhost:8800/v1/calc/run', files=locals(),
            data={'hazard_job_id': job_id})

        if response.ok:
            print(
                'Successfully sent calculation job to OpenQuake.')
            return response
        else:
            print(
                'Error sending the calculation job to OpenQuake.')
            return response
    except requests.exceptions.ConnectionError:
        print('Could not connect to OpenQuake')
        return abort(make_response(
            jsonify({'error': 'Could not connect to the OpenQuake API'}), 400))


def oqapi_get_job_status(job_id):
    try:
        return requests.get(f'http://localhost:8800/v1/calc/{job_id}/status')\
            .json()['status']
    except requests.exceptions.ConnectionError:
        print('Could not connect to OpenQuake')
        return abort(make_response(
            jsonify({'error': 'Could not connect to the OpenQuake API'}), 400))


def oqapi_wait_for_job(job_id):
    check_status = oqapi_get_job_status(job_id)

    while check_status != 'complete':
        time.sleep(1)
        check_status = oqapi_get_job_status(job_id)
        if check_status == 'failed':
            return make_response(check_status.json(), 400)
