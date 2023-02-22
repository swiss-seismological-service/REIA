import configparser
import json
import logging
import os
import time
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

import typer

from reia.actions import (create_risk_scenario, dispatch_openquake_calculation,
                          run_openquake_calculations)
from reia.datamodel import EEarthquakeType
from reia.db import crud, drop_db, init_db, session
from reia.io import CalculationBranchSettings, ERiskType
from reia.io.read import (parse_exposure, parse_fragility, parse_taxonomy_map,
                          parse_vulnerability)
from reia.io.write import (assemble_calculation_input, create_exposure_input,
                           create_fragility_input, create_taxonomymap_input,
                           create_vulnerability_input)
from settings import get_config

app = typer.Typer(add_completion=False)
db = typer.Typer()
exposure = typer.Typer()
vulnerability = typer.Typer()
fragility = typer.Typer()
taxonomymap = typer.Typer()
calculation = typer.Typer()
scenario = typer.Typer()

app.add_typer(db, name='db',
              help='Database Commands')
app.add_typer(exposure, name='exposure',
              help='Manage Exposure Models')
app.add_typer(vulnerability, name='vulnerability',
              help='Manage Vulnerability Models')
app.add_typer(fragility, name='fragility',
              help='Manage Fragility Models')
app.add_typer(taxonomymap, name='taxonomymap',
              help='Manage Taxonomy Mappings')
app.add_typer(calculation, name='calculation',
              help='Create or execute calculations')
app.add_typer(scenario, name='scenario',
              help='Manage Scenario Data')


@db.command('drop')
def drop_database():
    '''
    Drop all tables.
    '''
    drop_db()
    typer.echo('Tables dropped.')


@db.command('init')
def initialize_database():
    '''
    Create all tables.
    '''
    init_db()
    typer.echo('Tables created.')


@exposure.command('add')
def add_exposure(exposure: Path, name: str):
    '''
    Add an exposure model.
    '''
    with open(exposure, 'r') as f:
        exposure, assets = parse_exposure(f)

    exposure['name'] = name

    asset_collection = crud.create_asset_collection(exposure, session)

    asset_objects = crud.create_assets(assets, asset_collection._oid, session)
    sites = crud.read_sites(asset_collection._oid, session)

    typer.echo(f'Created asset collection with ID {asset_collection._oid} and '
               f'{len(sites)} sites with {len(asset_objects)} assets.')
    session.remove()


@exposure.command('delete')
def delete_exposure(asset_collection_oid: int):
    '''
    Delete an exposure model.
    '''
    deleted = crud.delete_asset_collection(asset_collection_oid, session)
    typer.echo(
        f'Deleted {deleted} asset collections with ID {asset_collection_oid}.')
    session.remove()


@exposure.command('list')
def list_exposure():
    '''
    List all exposure models.
    '''
    asset_collections = crud.read_asset_collections(session)

    typer.echo('List of existing asset collections:')
    typer.echo('{0:<10} {1:<25} {2}'.format(
        'ID',
        'Name',
        'Creationtime'))

    for ac in asset_collections:
        typer.echo('{0:<10} {1:<25} {2}'.format(
            ac._oid,
            ac.name or '',
            str(ac.creationinfo_creationtime)))
    session.remove()


@exposure.command('create_file')
def create_exposure(id: int, filename: Path):
    '''
    Create input files for an exposure model.
    '''
    p_xml = filename.with_suffix('.xml')
    p_csv = filename.with_suffix('.csv')
    fp_xml, fp_csv = create_exposure_input(id, session, assets_csv_name=p_csv)
    session.remove()

    p_xml.parent.mkdir(exist_ok=True)
    p_xml.open('w').write(fp_xml.getvalue())
    p_csv.open('w').write(fp_csv.getvalue())

    if p_xml.exists() and p_csv.exists():
        typer.echo(
            f'Successfully created file "{str(p_xml)}" and "{str(p_csv)}".')
    else:
        typer.echo('Error occurred, file was not created.')


@fragility.command('add')
def add_fragility(fragility: Path, name: str):
    '''
    Add a fragility model.
    '''

    with open(fragility, 'r') as f:
        model = parse_fragility(f)

    model['name'] = name

    fragility_model = crud.create_fragility_model(model, session)
    typer.echo(
        f'Created fragility model of type "{fragility_model._type}"'
        f' with ID {fragility_model._oid}.')
    session.remove()


@fragility.command('delete')
def delete_fragility(fragility_model_oid: int):
    '''
    Delete a fragility model.
    '''
    crud.delete_fragility_model(fragility_model_oid, session)
    typer.echo(
        f'Deleted fragility model with ID {fragility_model_oid}.')
    session.remove()


@fragility.command('list')
def list_fragility():
    '''
    List all fragility models.
    '''
    fragility_models = crud.read_fragility_models(session)

    typer.echo('List of existing fragility models:')
    typer.echo('{0:<10} {1:<25} {2:<50} {3}'.format(
        'ID',
        'Name',
        'Type',
        'Creationtime'))

    for vm in fragility_models:
        typer.echo('{0:<10} {1:<25} {2:<50} {3}'.format(
            vm._oid,
            vm.name or "",
            vm._type,
            str(vm.creationinfo_creationtime)))
    session.remove()


@fragility.command('create_file')
def create_fragility(id: int, filename: Path):
    '''
    Create input file for a fragility model.
    '''
    filename = filename.with_suffix('.xml')
    file_pointer = create_fragility_input(id, session)
    session.remove()

    filename.parent.mkdir(exist_ok=True)
    filename.open('w').write(file_pointer.getvalue())

    if filename.exists():
        typer.echo(
            f'Successfully created file "{str(filename)}".')
    else:
        typer.echo('Error occurred, file was not created.')


@taxonomymap.command('add')
def add_taxonomymap(map_file: Path, name: str):
    '''
    Add a taxonomy mapping model.
    '''
    with open(map_file, 'r') as f:
        mapping = parse_taxonomy_map(f)

    taxonomy_map = crud.create_taxonomy_map(mapping, name, session)
    typer.echo(
        f'Created taxonomy map with ID {taxonomy_map._oid}.')
    session.remove()


@taxonomymap.command('delete')
def delete_taxonomymap(taxonomymap_oid: int):
    '''
    Delete a vulnerability model.
    '''
    crud.delete_taxonomymap(taxonomymap_oid, session)
    typer.echo(
        f'Deleted vulnerability model with ID {taxonomymap_oid}.')
    session.remove()


@taxonomymap.command('list')
def list_taxonomymap():
    '''
    List all vulnerability models.
    '''
    taxonomy_maps = crud.read_taxonomymaps(session)

    typer.echo('List of existing vulnerability models:')
    typer.echo('{0:<10} {1:<25} {2}'.format(
        'ID',
        'Name',
        'Creationtime'))

    for tm in taxonomy_maps:
        typer.echo('{0:<10} {1:<25} {2}'.format(
            tm._oid,
            tm.name or "",
            str(tm.creationinfo_creationtime)))
    session.remove()


@taxonomymap.command('create_file')
def create_taxonomymap(id: int, filename: Path):
    '''
    Create input file for a taxonomy mapping.
    '''
    filename = filename.with_suffix('.csv')
    file_pointer = create_taxonomymap_input(id, session)
    session.remove()

    filename.parent.mkdir(exist_ok=True)
    filename.open('w').write(file_pointer.getvalue())

    if filename.exists():
        typer.echo(
            f'Successfully created file "{str(filename)}".')
    else:
        typer.echo('Error occurred, file was not created.')


@vulnerability.command('add')
def add_vulnerability(vulnerability: Path, name: str):
    '''
    Add a vulnerability model.
    '''
    with open(vulnerability, 'r') as f:
        model = parse_vulnerability(f)
    model['name'] = name

    vulnerability_model = crud.create_vulnerability_model(model, session)

    typer.echo(
        f'Created vulnerability model of type "{vulnerability_model._type}"'
        f' with ID {vulnerability_model._oid}.')
    session.remove()


@vulnerability.command('delete')
def delete_vulnerability(vulnerability_model_oid: int):
    '''
    Delete a vulnerability model.
    '''
    crud.delete_vulnerability_model(vulnerability_model_oid, session)
    typer.echo(
        f'Deleted vulnerability model with ID {vulnerability_model_oid}.')
    session.remove()


@vulnerability.command('list')
def list_vulnerability():
    '''
    List all vulnerability models.
    '''
    vulnerability_models = crud.read_vulnerability_models(session)

    typer.echo('List of existing vulnerability models:')
    typer.echo('{0:<10} {1:<25} {2:<50} {3}'.format(
        'ID',
        'Name',
        'Type',
        'Creationtime'))

    for vm in vulnerability_models:
        typer.echo('{0:<10} {1:<25} {2:<50} {3}'.format(
            vm._oid,
            vm.name or "",
            vm._type,
            str(vm.creationinfo_creationtime)))
    session.remove()


@vulnerability.command('create_file')
def create_vulnerability(id: int, filename: Path):
    '''
    Create input file for a vulnerability model.
    '''
    filename = filename.with_suffix('.xml')
    file_pointer = create_vulnerability_input(id, session)
    session.remove()

    filename.parent.mkdir(exist_ok=True)
    filename.open('w').write(file_pointer.getvalue())

    if filename.exists():
        typer.echo(
            f'Successfully created file "{str(filename)}".')
    else:
        typer.echo('Error occurred, file was not created.')


@calculation.command('create_files')
def create_calculation_files(target_folder: Path,
                             settings_file: Path):
    '''
    Create all files for an OpenQuake calculation.
    '''
    target_folder.mkdir(exist_ok=True)

    job_file = configparser.ConfigParser()
    job_file.read(settings_file)

    files = assemble_calculation_input(job_file, session)

    for file in files:
        with open(target_folder / file.name, 'w') as f:
            f.write(file.getvalue())

    typer.echo('Openquake calculation files created '
               f'in folder "{str(target_folder)}".')

    session.remove()


@calculation.command('run_test')
def run_test_calculation(settings_file: Path):
    '''
    Send a calculation to OpenQuake as a test.
    '''
    job_file = configparser.ConfigParser()
    job_file.read(settings_file)

    response = dispatch_openquake_calculation(job_file, session)

    typer.echo(response.json())

    session.remove()


@calculation.command('run')
def run_calculation(
        earthquake_file: typer.FileText,
        settings: list[str] = typer.Option([]),
        weights: list[float] = typer.Option([])):
    '''
    Run an OpenQuake calculation.
    '''

    # console input validation
    if settings and not len(settings) == len(weights):
        typer.echo('Error: Number of setting files and weights provided '
                   'have to be equal. Exiting...')
        raise typer.Exit(code=1)

    # input parsing
    settings = zip(weights, settings) if settings \
        else get_config().OQ_SETTINGS

    branch_settings = []
    for s in settings:
        job_file = configparser.ConfigParser()
        job_file.read(Path(s[1]))
        branch_settings.append(CalculationBranchSettings(s[0], job_file))

    # create or update earthquake
    earthquake_oid = crud.create_or_update_earthquake_information(
        json.loads(earthquake_file.read()), session)

    run_openquake_calculations(branch_settings, earthquake_oid, session)

    session.remove()


@calculation.command('list')
def list_calculations():
    '''
    List all calculations.
    '''
    calculations = crud.read_calculations(session)

    typer.echo('List of existing calculations:')
    typer.echo('{0:<10} {1:<25} {2:<25} {3:<30} {4}'.format(
        'ID',
        'Status',
        'Type',
        'Created',
        'Description'))

    for c in calculations:
        typer.echo('{0:<10} {1:<25} {2:<25} {3:<30} {4}'.format(
            c._oid,
            c.status,
            c._type,
            str(c.creationinfo_creationtime),
            c.description))
    session.remove()


@scenario.command('list')
def list_scenario():
    '''
    List all scenarios.
    '''
    scenarios = crud.read_scenario_calculations(session)

    typer.echo('List of existing scenarios:')
    typer.echo('{0:<10} {1:<25} {2:<25} {3:<30} {4}'.format(
        'ID',
        'Status',
        'Type',
        'Created',
        'Description'))

    for sc in scenarios:
        typer.echo('{0:<10} {1:<25} {2:<25} {3:<30} {4}'.format(
            sc._oid,
            sc.status,
            sc._type,
            str(sc.creationinfo_creationtime),
            sc.description))

    session.remove()


@scenario.command('delete')
def delete_scenario(scenario_oid: int):
    '''
    Delete a scenario calculation
    '''
    deleted = crud.delete_scenario_calculation(scenario_oid, session)
    typer.echo(
        f'Deleted {deleted} scenario calculation with '
        f'ID {scenario_oid}.')
    session.remove()


@scenario.command('add')
def add_scenario(config: typer.FileText):
    '''
    Add scenario data.
    '''

    scenario_configs = json.loads(config.read())

    os.makedirs('logs', exist_ok=True)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - [%(filename)s.%(funcName)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[TimedRotatingFileHandler('logs/datapipe.log',
                                           when='d',
                                           interval=1,
                                           backupCount=5),
                  logging.StreamHandler()
                  ]
    )
    LOGGER = logging.getLogger(__name__)

    start = time.perf_counter()

    aggregation_tags = {}

    for type in ['Canton', 'CantonGemeinde']:
        existing_tags = crud.read_aggregationtags(type, session)
        aggregation_tags.update({t.name: t for t in existing_tags})

    for config in scenario_configs:
        start_scenario = time.perf_counter()
        LOGGER.info(f'Starting to parse scenario {config["scenario_name"]}.')
        earthquake_oid = crud.create_or_update_earthquake_information(
            {'type': EEarthquakeType.SCENARIO, 'originid': config['originid']},
            session)

        LOGGER.info('Creating risk scenarios....')
        create_risk_scenario(earthquake_oid,
                             ERiskType.LOSS,
                             aggregation_tags,
                             config,
                             session)

        LOGGER.info('Creating damage scenarios....')
        create_risk_scenario(earthquake_oid,
                             ERiskType.DAMAGE,
                             aggregation_tags,
                             config,
                             session)

        LOGGER.info(
            'Saving the scenario took '
            f'{int((time.perf_counter()-start_scenario)/60)} minutes. Running '
            f'for a total of {int((time.perf_counter()-start)/60/60)} hours.')

    session.remove()

    LOGGER.info(
        'Saving all results took '
        f'{int((time.perf_counter()-start)/60/60)} hours.')
