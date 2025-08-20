import signal
from io import StringIO
from pathlib import Path

from reia.config.settings import get_settings
from reia.services.oq_api import OQCalculationAPI


def test_api():
    def timeout_handler(signum, frame):
        raise TimeoutError("Test timed out after 60 seconds")

    signal.signal(signal.SIGALRM, timeout_handler)
    signal.alarm(60)  # 1 minute timeout

    try:
        datafolder = Path(__file__).parent / 'data' / 'oq_test'

        with open(datafolder / 'exposure_model.xml', 'r') as f:
            exposure = StringIO(f.read())
            exposure.name = 'exposure_model.xml'
        with open(datafolder / 'gmf_scenario.csv', 'r') as f:
            gmf_scenario = StringIO(f.read())
            gmf_scenario.name = 'gmf_scenario.csv'
        with open(datafolder / 'job_risk.ini', 'r') as f:
            job_risk = StringIO(f.read())
            job_risk.name = 'job.ini'
        with open(datafolder / 'sites.csv', 'r') as f:
            sites = StringIO(f.read())
            sites.name = 'sites.csv'
        with open(datafolder / 'vulnerability.xml', 'r') as f:
            vulnerability = StringIO(f.read())
            vulnerability.name = 'vulnerability.xml'

        api = OQCalculationAPI(get_settings())
        api.add_calc_files(
            exposure,
            gmf_scenario,
            job_risk,
            sites,
            vulnerability)
        final_status = api.run()

        assert final_status == 'complete', "Expected 'complete' " \
            f"but got '{final_status}'"
    finally:
        signal.alarm(0)  # Disable the alarm
