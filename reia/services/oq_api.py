import io
import logging
import sys
import time
import zipfile

import requests
from openquake.calculators.extract import WebExtractor
from openquake.commonlib import datastore
from openquake.engine import engine
from openquake.server import dbserver

from reia.config.settings import REIASettings


class APIConnection():
    def __init__(self,
                 server: str,
                 auth: dict | None = None,
                 logger_name: str = '__name__'):
        self.server = server
        self.auth = auth
        self.logger = logging.getLogger(logger_name)

        self.session = requests.Session()
        if self.auth:
            self.authenticate()

    def authenticate(self):
        try:
            response = self.session.post(f'{self.server}/accounts/ajax_login/',
                                         data=self.auth)
            response.raise_for_status()
        except requests.exceptions.RequestException:
            self.logger.warning(
                "Authentication failed - "
                "OpenQuake may not have LOCKDOWN enabled")


class OQCalculationAPI(APIConnection):
    def __init__(self, config: REIASettings):
        super().__init__(config.oq_host, config.oq_api_auth, 'openquake')

        self.url = f'{self.server}/v1/calc'
        self.files = None
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
            time.sleep(5)
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

    def log_error_with_traceback(
            self, context: str = "OpenQuake calculation failed") -> None:
        """Log calculation error with OpenQuake traceback.

        Args:
            context: Context message for the error
        """
        try:
            traceback_lines = self.get_traceback()

            if traceback_lines:
                traceback_text = "\n".join(traceback_lines)
                self.logger.error(
                    f"{context} (calc_id: {self.id})\n"
                    f"OpenQuake Traceback:\n{traceback_text}"
                )
            else:
                self.logger.error(
                    f"{context} (calc_id: {self.id}) - No traceback available")

        except Exception as e:
            self.logger.error(
                f"{context} (calc_id: {self.id}) - "
                f"Failed to fetch traceback: {e}")

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

        Creates a single zip archive containing all files and
        stores it for upload.

        Args:
            *args: IO objects representing calculation files
        """
        if not args:
            raise ValueError("At least one file must be provided")

        # Create a zip file in memory
        zip_buffer = io.BytesIO()

        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for file_obj in args:
                filename = getattr(file_obj, 'name', 'unnamed_file')

                file_obj.seek(0)
                file_content = file_obj.read()

                # Add file to zip
                zip_file.writestr(filename, file_content)

        zip_buffer.seek(0)

        self.files = {'archive':
                      ('calculation.zip', zip_buffer, 'application/zip')}

    def get_result(self) -> datastore.DataStore:
        """Get calculation results as datastore.

        Returns:
            OpenQuake datastore with calculation results
        """
        oqapi_import_remote_calculation(self.id, self.config)

        return datastore.read(self.id)


def oqapi_import_remote_calculation(
        calc_id: int | str,
        config: REIASettings):
    """Import a remote calculation into the local database.

    Args:
        calc_id: Usually a integer id of a remote calculation.
                Can be a local pathname to a datastore not already
                present in the database: in that case it is imported in the db.
        config: Configuration object containing API settings.

    Note:
        calc_id can be a local pathname to a datastore not already
        present in the database: in that case it is imported in the db.
    """
    from reia.services.logger import LoggerService
    logger = LoggerService.get_logger(__name__)

    # TODO: Error handling and logs
    dbserver.ensure_on()
    logger.info('Starting import of %s...', calc_id)
    try:
        calc_id = int(calc_id)
    except ValueError:  # assume calc_id is a pathname
        pass
    else:
        logger.info('Importing remote calculation %s...', calc_id)
        datadir = datastore.get_datadir()
        auth = config.oq_api_auth

        # Pass None for credentials if they're empty (no LOCKDOWN)
        username = auth.get('username') if auth.get('username') else None
        password = auth.get('password') if auth.get('password') else None
        webex = WebExtractor(
            calc_id,
            config.oq_host,
            username,
            password)

        # Extraction does not work for child calculations of a hazard calc
        hc_id = webex.oqparam.hazard_calculation_id
        if hc_id:
            sys.exit('The job has a parent (#%d) and cannot be '
                     'downloaded.' % hc_id)

        # Download the datastore, overwrites any existing datastore with
        # the same ID.
        datastore_path = '%s/calc_%d.hdf5' % (datadir, calc_id)
        try:
            webex.dump(datastore_path)
        except Exception as e:
            logger.error(
                f"Failed to download datastore for calc {calc_id}: {e}")
            raise
        finally:
            webex.close()

        # If a calculation already exists with this ID, the datastore got
        # overwritten, we need to delete the entry in the DB so it can be
        # updated.
        # TODO: Check whether this works if a local OQ installation is used
        #       instead of a remote/dockerized one.
        try:
            existing_calc = dbserver.actions.calc_info(dbserver.db, calc_id)
        except Exception:
            pass
        else:
            logger.info(f'Another calculation with id {calc_id} is already '
                        'present in the local database, replacing...')

            dbserver.actions.del_calc(dbserver.db,
                                      calc_id,
                                      existing_calc['user_name'],
                                      delete_file=False,
                                      force=True)

    with datastore.read(calc_id) as dstore:
        engine.expose_outputs(dstore, status='complete')

    logger.info('Imported calculation %s successfully', calc_id)
