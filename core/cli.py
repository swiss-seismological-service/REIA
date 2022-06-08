import typer
from pathlib import Path
from core.input import create_vulnerability_input

from core.parsers import parse_exposure, parse_vulnerability
from core.db import drop_db, init_db, session
from core.db.crud import (
    create_asset_collection,
    create_assets,
    create_vulnerability_model,
    delete_asset_collection,
    delete_vulnerability_model,
    read_asset_collections,
    read_sites,
    read_vulnerability_models)


app = typer.Typer(add_completion=False)
db = typer.Typer()
exposure = typer.Typer()
vulnerability = typer.Typer()

app.add_typer(db, name='db', help='Database Commands')
app.add_typer(exposure, name='exposure', help='Manage Exposure Models')
app.add_typer(vulnerability, name='vulnerability',
              help='Manage Vulnerability Models')


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
            ac.name,
            ac.creationinfo_creationtime))
    session.remove()


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
            vm.creationinfo_creationtime))
    session.remove()


@vulnerability.command('create')
def create_vulnerability(id: int, filename: Path):
    file_pointer = create_vulnerability_input(id, session)
    session.remove()

    p = Path(filename)
    p.parent.mkdir(exist_ok=True)

    p.open('w').write(file_pointer.getvalue())
    if p.exists():
        typer.echo(f'Successfully created file "{str(p)}".')
    else:
        typer.echo('Error occurred, file was not created.')
