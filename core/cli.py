import configparser
from typing import Optional
import typer
from pathlib import Path
from core.actions import execute_openquake_calculation
from core.oqapi import (oqapi_send_calculation)

from settings import get_config

from core.input import (assemble_calculation_input,
                        create_exposure_input,
                        create_vulnerability_input)
from core.parsers import (
    parse_exposure,
    parse_calculation,
    parse_vulnerability)
from core.db import drop_db, init_db, session
from core.db.crud import (create_asset_collection,
                          create_assets, create_calculation,
                          create_vulnerability_model,
                          delete_asset_collection,
                          delete_vulnerability_model,
                          read_asset_collections, read_calculations,
                          read_sites,
                          read_vulnerability_models,
                          EStatus)


app = typer.Typer(add_completion=False)
db = typer.Typer()
exposure = typer.Typer()
vulnerability = typer.Typer()
calculation = typer.Typer()

app.add_typer(db, name='db', help='Database Commands')
app.add_typer(exposure, name='exposure', help='Manage Exposure Models')
app.add_typer(vulnerability, name='vulnerability',
              help='Manage Vulnerability Models')
app.add_typer(calculation, name='calculation',
              help='Create or execute calculations')


@db.command('drop')
def drop_database():
    '''Drops all tables.'''
    drop_db()
    typer.echo('Tables Dropped')


@db.command('init')
def initialize_database():
    '''Creates all tables.'''
    init_db()
    typer.echo('Tables created')


@exposure.command('add')
def add_exposure(exposure: Path, name: str):
    '''Allows to add an exposure model to the database. '''

    with open(exposure, 'r') as f:
        exposure, assets = parse_exposure(f)

    exposure['name'] = name

    asset_collection = create_asset_collection(exposure, session)

    asset_objects = create_assets(assets, asset_collection, session)
    sites = read_sites(asset_collection._oid, session)

    typer.echo(f'Created asset collection with ID {asset_collection._oid} and '
               f'{len(sites)} sites with {len(asset_objects)} assets.')
    session.remove()


@exposure.command('delete')
def delete_exposure(asset_collection_oid: int):
    deleted = delete_asset_collection(asset_collection_oid, session)
    typer.echo(
        f'Deleted {deleted} asset collections with ID {asset_collection_oid}.')
    session.remove()


@exposure.command('list')
def list_exposure():
    asset_collections = read_asset_collections(session)

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


@exposure.command('create')
def create_exposure(id: int, filename: Path):
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
    '''Allows to add an vulnerability model to the database. '''

    with open(vulnerability, 'r') as f:
        model = parse_vulnerability(f)
    model['name'] = name
    vulnerability_model = create_vulnerability_model(model, session)

    typer.echo(
        f'Created vulnerability model of type "{vulnerability_model._type}"'
        f' with ID {vulnerability_model._oid}.')
    session.remove()


@vulnerability.command('delete')
def delete_vulnerability(vulnerability_model_oid: int):
    deleted = delete_vulnerability_model(vulnerability_model_oid, session)
    typer.echo(
        f'Deleted {deleted} vulnerability models with '
        f'ID {vulnerability_model_oid}.')
    session.remove()


@vulnerability.command('list')
def list_vulnerability():
    vulnerability_models = read_vulnerability_models(session)

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


@vulnerability.command('create')
def create_vulnerability(id: int, filename: Path):
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

    job_file = configparser.ConfigParser()
    job_file.read(settings_file or Path(get_config().OQ_SETTINGS))

    files = assemble_calculation_input(job_file, session)

    response = oqapi_send_calculation(*files)

    typer.echo(response.json())

    session.remove()


@calculation.command('run')
def run_calculation(settings_file: Optional[Path] = typer.Argument(None)):

    job_file = configparser.ConfigParser()
    job_file.read(settings_file or Path(get_config().OQ_SETTINGS))

    # save calculation to database
    calculation_dict = parse_calculation(job_file)
    # create calculation files
    files = assemble_calculation_input(job_file, session)

    calculation_dict['status'] = EStatus.DISPATCHED
    calculation = create_calculation(calculation_dict, session)

    execute_openquake_calculation(files, calculation, session)

    typer.echo(
        f'Calculation finished with status "{EStatus(calculation.status)}".')

    session.remove()


@calculation.command('list')
def list_calculations():
    calculations = read_calculations(session)

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
