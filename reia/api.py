import io
import logging
import sys
import time

import requests
from openquake.calculators.extract import WebExtractor
from openquake.commonlib import datastore, logs
from openquake.engine import engine
from openquake.server import dbserver

from settings.config import Config


class APIConnection():
    def __init__(self, server: str, auth: dict, logger_name: str = '__name__'):
        self.server = server
        self.auth = auth
        self.logger = logging.getLogger(logger_name)

        self.session = requests.Session()
        self.authenticate()

    def authenticate(self):
        self.session.post(f'{self.server}/accounts/ajax_login/',
                          data=self.auth)


class OQCalculationAPI(APIConnection):
    def __init__(self, config: Config):
        super().__init__(config.OQ_API_SERVER, config.OQ_API_AUTH, 'openquake')

        self.url = f'{self.server}/v1/calc'
        self.files = {}
        self.config = config

        self.id = None
        self.status = None
        self.mode = None
        self.is_running = False
        self.abortable = False

    def submit(self) -> dict:
        """Submit calculation to OpenQuake without waiting.

        Returns:
            Response dictionary with job_id and initial status
        """
        if not self.files:
            raise ValueError(
                'No calculation files provided. Call add_calc_files() first.')

        response = self.session.post(f'{self.url}/run', files=self.files)
        response.raise_for_status()
        response_data = response.json()

        self.id = response_data['job_id']
        self.status = response_data['status']

        return response_data

    def run(self) -> str:
        """Submit calculation to OpenQuake and wait for completion.

        Returns:
            Final calculation status
        """
        # Submit calculation
        self.submit()

        # Monitor until completion
        while self.status not in ['complete', 'aborted', 'failed']:
            time.sleep(1)
            self.get_status()

        return self.status

    def get_status(self) -> str:
        """Get current calculation status from OpenQuake.

        Returns:
            Current status string
        """
        if self.id is None:
            raise ValueError('No calculation dispatched yet.')

        response = self.session.get(f'{self.url}/{self.id}/status')
        response.raise_for_status()
        response = response.json()

        self.status = response['status']
        self.mode = response['calculation_mode']
        self.is_running = response['is_running']
        self.abortable = response['abortable']

        # WORKAROUND: OQ fails if loss calculation produces no values
        if self.status == 'failed':
            self.check_failed_status()

        return self.status

    def check_failed_status(self) -> None:
        """Check if failure is due to empty risk
        table and mark as complete if so.
        """
        # WORKAROUND: OQ fails if loss calculation produces no values
        empty = 'SystemExit: The risk_by_event table is empty!'
        if any(empty in t for t in self.get_traceback()):
            self.status = 'complete'

    def get_traceback(self):
        """Get traceback information for failed calculations."""
        response = self.session.get(f'{self.url}/{self.id}/traceback')
        response.raise_for_status()
        return response.json()

    def abort(self) -> str:
        """Abort the running calculation.

        Returns:
            Updated status after abort
        """
        if self.id is None:
            raise ValueError('No calculation dispatched yet.')
        if not self.abortable:
            raise ValueError('Calculation is not abortable.')

        response = self.session.post(f'{self.url}/{self.id}/abort')
        response.raise_for_status()

        self.get_status()  # Update status after abort
        return self.status

    def add_calc_files(self, *args: io.StringIO) -> None:
        """Add calculation files for submission to OpenQuake.

        Args:
            *args: IO objects representing calculation files
        """
        args = list(args)
        job_config_index = next(
            (i for i, f in enumerate(args) if f.name == 'job.ini'), None)

        if job_config_index is not None:
            job_config = args.pop(job_config_index)
            self.files['job_config'] = job_config

        self.files = self.files | {
            f'input_model_{i + 1}': v for i, v in enumerate(args)}

    def get_result(self) -> datastore.DataStore:
        """Get calculation results as datastore.

        Returns:
            OpenQuake datastore with calculation results
        """
        dbserver.ensure_on()

        # if id doesn not exist locally, try getting it on remote
        job = logs.dbcmd('get_job', self.id)
        if job is None:
            oqapi_import_remote_calculation(self.id, self.config)

        return datastore.read(self.id)


def oqapi_import_remote_calculation(calc_id: int | str, config: Config):
    """Import a remote calculation into the local database.

    Args:
        calc_id: Can be a local pathname to a datastore not already
                present in the database: in that case it is imported in the db.
        config: Configuration object containing API settings.

    Note:
        calc_id can be a local pathname to a datastore not already
        present in the database: in that case it is imported in the db.
    """
    # TODO: Error handling and logs
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
        engine.expose_outputs(dstore, status='complete')
    logging.info('Imported calculation %s successfully', calc_id)
