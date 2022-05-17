import io
import time

from flask.helpers import make_response
from flask.json import jsonify
import numpy
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


class WebAPIError(RuntimeError):
    """
    Wrapper for an error on a WebAPI server
    """


class WebExtractor():
    """
    A class to extract data from the WebAPI.

    :param calc_id: a calculation ID
    :param server: hostname of the webapi server (can be '')
    :param username: login username (can be '')
    :param password: login password (can be '')

    NB: instantiating the WebExtractor opens a session.
    """

    def __init__(self, calc_id, server, username=None, password=None):
        self.calc_id = calc_id
        self.server = server
        self.sess = requests.Session()
        if username:
            login_url = '%s/accounts/ajax_login/' % self.server
            print('POST %s', login_url)
            resp = self.sess.post(
                login_url, data=dict(username=username, password=password))
            if resp.status_code != 200:
                raise WebAPIError(resp.text)
        url = '%s/v1/calc/%d/extract/oqparam' % (self.server, calc_id)
        print('GET %s', url)
        resp = self.sess.get(url)
        if resp.status_code == 404:
            raise WebAPIError('Not Found: %s' % url)
        elif resp.status_code != 200:
            raise WebAPIError(resp.text)

    def get(self, what):
        """
        :param what: what to extract
        :returns: an ArrayWrapper instance
        """
        url = '%s/v1/calc/%d/extract/%s' % (self.server, self.calc_id, what)
        # url = 'http://localhost:8800/v1/calc/result/17'
        print('GET %s', url)
        resp = self.sess.get(url)
        if resp.status_code != 200:
            raise WebAPIError(resp.text)
        print('Read of data')
        npz = numpy.load(io.BytesIO(resp.content))

        attrs = {k: npz[k] for k in npz if k != 'array'}
        try:
            arr = npz['array']
        except KeyError:
            arr = ()
        return arr, attrs

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def dump(self, fname):
        """
        Dump the remote datastore on a local path.
        """
        CHUNKSIZE = 4 * 1024**2  # 4 MB
        url = '%s/v1/calc/%d/datastore' % (self.server, self.calc_id)
        resp = self.sess.get(url, stream=True)
        down = 0
        with open(fname, 'wb') as f:
            print('Saving %s', fname)
            for chunk in resp.iter_content(CHUNKSIZE):
                f.write(chunk)
                down += len(chunk)
                print('Downloaded {:,} bytes'.format(down))

    def close(self):
        """
        Close the session
        """
        self.sess.close()
