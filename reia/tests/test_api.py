import time
from io import StringIO

from reia.api import OQCalculationAPI
from settings import get_config


def test_api():
    with open('reia/tests/data/exposure_model.xml', 'r') as f:
        exposure = StringIO(f.read())
        exposure.name = 'exposure_model.xml'
    with open('reia/tests/data/gmf_scenario.csv', 'r') as f:
        gmf_scenario = StringIO(f.read())
        gmf_scenario.name = 'gmf_scenario.csv'
    with open('reia/tests/data/job_risk.ini', 'r') as f:
        job_risk = StringIO(f.read())
        job_risk.name = 'job.ini'
    with open('reia/tests/data/sites.csv', 'r') as f:
        sites = StringIO(f.read())
        sites.name = 'sites.csv'
    with open('reia/tests/data/vulnerability.xml', 'r') as f:
        vulnerability = StringIO(f.read())
        vulnerability.name = 'vulnerability.xml'

    api = OQCalculationAPI(get_config())
    api.add_calc_files(exposure, gmf_scenario, job_risk, sites, vulnerability)
    api.run()
    while api.status not in ['complete', 'aborted', 'failed']:
        print(api.status)
        time.sleep(1)
    print(api.status)
    print(api.get_result())
