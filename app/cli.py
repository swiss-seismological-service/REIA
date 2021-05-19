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
        'http://localhost:8800/v1/calc/run', files=files2, data={'hazard_job_id': -1})

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
    with logs.init('job', '/mnt/c/workspaces/SED/files-event-specific-loss/oq_calculations/test_calculation/prepare_job_mmi.ini') as log:  # initialize logs
        calc = calculators(log.get_oqparam(), log.calc_id)
        calc.run()  # run the calculator
        h_id = log.calc_id

    with logs.init('job', '/mnt/c/workspaces/SED/files-event-specific-loss/oq_calculations/test_calculation/job.ini', hc_id=h_id) as log:  # initialize logs
        calc = calculators(log.get_oqparam(), log.calc_id)
        calc.run()  # run the calculator

    # @app.cli.group()
    # def translate():
    #     """Translation and localization commands."""
    #     pass

    # @translate.command()
    # @click.argument('lang')
    # def init(lang):
    #     """Initialize a new language."""
    #     if os.system('pybabel extract -F babel.cfg -k _l -o messages.pot .'):
    #         raise RuntimeError('extract command failed')
    #     if os.system(
    #             'pybabel init -i messages.pot -d project/translations -l ' + lang):
    #         raise RuntimeError('init command failed')
    #     os.remove('messages.pot')

    # @translate.command()
    # def update():
    #     """Update all languages."""
    #     if os.system('pybabel extract -F babel.cfg -k _l -o messages.pot .'):
    #         raise RuntimeError('extract command failed')
    #     if os.system('pybabel update -i messages.pot -d project/translations'):
    #         raise RuntimeError('update command failed')
    #     os.remove('messages.pot')

    # @translate.command()
    # def compile():
    #     """Compile all languages."""
    #     if os.system('pybabel compile -d project/translations'):
    #         raise RuntimeError('compile command failed')
