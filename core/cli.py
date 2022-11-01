import configparser
import json
from pathlib import Path
from typing import Optional

import typer
from esloss.datamodel.calculations import EStatus

from core.actions import (dispatch_openquake_calculation,
                          monitor_openquake_calculation,
                          save_openquake_results)
from core.db import crud, drop_db, init_db, session
from core.io.create_input import (assemble_calculation_input,
                                  create_exposure_input,
                                  create_vulnerability_input)
from core.io.parse_input import (parse_calculation, parse_exposure,
                                 parse_vulnerability,
                                 validate_calculation_input)
from core.utils import CalculationBranchSettings
from settings import get_config

app = typer.Typer(add_completion=False)
db = typer.Typer()
exposure = typer.Typer()
vulnerability = typer.Typer()
calculation = typer.Typer()

app.add_typer(db, name='db',
              help='Database Commands')
app.add_typer(exposure, name='exposure',
              help='Manage Exposure Models')
app.add_typer(vulnerability, name='vulnerability',
              help='Manage Vulnerability Models')
app.add_typer(calculation, name='calculation',
              help='Create or execute calculations')


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
    deleted = crud.delete_vulnerability_model(vulnerability_model_oid, session)
    typer.echo(
        f'Deleted {deleted} vulnerability models with '
        f'ID {vulnerability_model_oid}.')
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
def create_calculation_files(
        target_folder: Path,
        settings_file: Optional[Path] = typer.Argument(None)):
    '''
    Create all files for a OpenQuake calculation.
    '''
    target_folder.mkdir(exist_ok=True)

    if not settings_file:
        config = get_config()
        settings_file = Path(config.OQ_SETTINGS)

    files = assemble_calculation_input(settings_file, session)

    for file in files:
        with open(target_folder / file.name, 'w') as f:
            f.write(file.getvalue())
    typer.echo('Openquake calculation files created '
               f'in folder "{str(target_folder)}".')
    session.remove()


@calculation.command('run_test')
def run_test_calculation(settings_file: Optional[Path] = typer.Argument(None)):
    '''
    Send a calculation to OpenQuake as a test.
    '''
    job_file = configparser.ConfigParser()
    job_file.read(settings_file or Path(get_config().OQ_SETTINGS))

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
    settings = zip(weights, settings) if settings else get_config().OQ_SETTINGS
    branch_settings = []
    for s in settings:
        job_file = configparser.ConfigParser()
        job_file.read(Path(s[1]))
        branch_settings.append(CalculationBranchSettings(s[0], job_file))

    validate_calculation_input(branch_settings)

    # create or update earthquake
    earthquake_oid = crud.create_or_update_earthquake_information(
        json.loads(earthquake_file.read()), session)

    calculation_dict, branches_dicts = parse_calculation(branch_settings)
    calculation_dict['_earthquakeinformation_oid'] = earthquake_oid

    calculation = crud.create_calculation(calculation_dict, session)

    branches = [crud.create_calculation_branch(b, session, calculation._oid)
                for b in branches_dicts]

    print(calculation)
    print(branches)
    return

    # send calculation to OQ and keep updating its status
    try:
        response = dispatch_openquake_calculation(job_file, session)
        job_id = response.json()['job_id']
        monitor_openquake_calculation(job_id, calculation._oid, session)
    except BaseException as e:
        crud.update_calculation_status(
            calculation._oid, EStatus.FAILED, session)
        session.remove()
        raise e
    typer.echo(
        f'Calculation finished with status "{EStatus(calculation.status)}".')

    # Collect OQ results and save to database
    if calculation.status == EStatus.COMPLETE:
        save_openquake_results(calculation, job_id, session)

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
