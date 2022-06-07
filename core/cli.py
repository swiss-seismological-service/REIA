import typer
from pathlib import Path

from core.parsers import parse_exposure, parse_vulnerability
from core.db import drop_db, init_db, session
from core.db.crud import (
    create_asset_collection,
    create_assets,
    create_vulnerability_model,
    delete_asset_collection,
    read_asset_collections,
    read_sites)


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
    session.remove()
    typer.echo(f'Created asset collection with ID {asset_collection._oid} and '
               f'{len(sites)} sites with {len(asset_objects)} assets.')


@exposure.command('delete')
def delete_exposure(asset_collection: int):
    deleted = delete_asset_collection(asset_collection, session)
    session.remove()
    typer.echo(
        f'Deleted {deleted} asset collection with ID {asset_collection}.')


@exposure.command('list')
def list_exposure():
    asset_collections = read_asset_collections(session)
    session.remove()

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


# @vulnerability.command('add')
# def add_vulnerability(vulnerability: Path):
#     '''Allows to add an vulnerability model to the database. '''

#     with open(vulnerability, 'r') as f:
#         model, functions = parse_oq_vulnerability_file(f)

#     vm_oid = create_vulnerability_model(model, functions)

#     typer.echo(f'Created vulnerability model with ID {vm_oid}.')


# @vulnerability.command('delete')
# def delete_vulnerability(vulnerability_model: int):
#     typer.echo(f'Deleted vulnerability model {vulnerability_model}.')


# @vulnerability.command('list')
# def list_vulnerability():
#     typer.echo('List of existing vulnerability models:')
