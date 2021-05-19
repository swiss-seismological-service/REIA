from app import app
from datamodel import *
from datamodel.base import init_db, drop_db
import requests


@app.cli.group()
def db():
    """Database Commands"""
    pass


@db.command()
def drop():
    """Drop connected database"""
    drop_db()
    return 'Database dropped'


@db.command()
def init():
    """Initiate specified database"""
    init_db()
    return 'Database successfully initiated'


@app.cli.group()
def oqapi():
    """call OQ API Commands"""
    pass


@oqapi.command()
def list():
    response = requests.get('http://localhost:8800/v1/calc/list')
    print(response.text)


@oqapi.command()
def run():
    bpth = '/mnt/c/workspaces/SED/files-event-specific-loss' \
        '/oq_calculations/test_calculation/'

    files1 = {
        'job_config': open(bpth + 'prepare_job_mmi.ini', 'rb'),
        'input_model_1': open(bpth + 'Exposure_dummy2.xml', 'rb'),
        'input_model_2': open(bpth + 'Exposure_dummy2.csv', 'rb'),
        'input_model_3': open(bpth + 'structural_vulnerability_'
                              'model_real_MMI_shift.xml', 'rb')
    }

    files2 = {
        'job_config': open(bpth + 'risk.ini', 'rb'),
        'input_model_1': open(bpth + 'shakemap_files/grid.zip', 'rb')
    }

    response = requests.post(
        'http://localhost:8800/v1/calc/run', files=files1)

    response = requests.post(
        'http://localhost:8800/v1/calc/run', files=files2,
        data={'hazard_job_id': -1})

    if response.ok:
        print("Upload completed successfully!")
        print(response.text)
    else:
        print("Something went wrong!")
        print(response.text)


@oqapi.command()
def run_python():
    from openquake.commonlib import logs
    from openquake.calculators.base import calculators
    h_id = 0

    with logs.init(
        'job', '/mnt/c/workspaces/SED/files-event-specific-loss/'
            'oq_calculations/test_calculation/prepare_job_mmi.ini') as log:
        calc = calculators(log.get_oqparam(), log.calc_id)
        calc.run()  # run the calculator
        h_id = log.calc_id

    with logs.init(
        'job', '/mnt/c/workspaces/SED/files-event-specific-loss/'
            'oq_calculations/test_calculation/job.ini', hc_id=h_id) as log:
        calc = calculators(log.get_oqparam(), log.calc_id)
        calc.run()  # run the calculator


@app.cli.group()
def read():
    """read model"""
    pass


@read.command()
def exposure():
    # TODO: read exposure.json into AssetCollection

    # TODO: read exposure.csv and parse to assets and sites

    # TODO: make all necessary relationships to assetCollection

    pass


@ read.command()
def vulnerability():
    # TODO: read vulnerability xml to VulnerabilityModel

    # TODO: read vulnerability xml to VulnerabilityFunctions

    pass
