import typer
# from pathlib import Path
# from core.crud import create_asset_collection, create_vulnerability_model
from core.db import drop_db, init_db
# from core.parsers import (
#     parse_asset_csv,
#     parse_oq_exposure_file,
#     parse_oq_vulnerability_file)

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


# @exposure.command('add')
# def add_exposure(exposure: Path, assets: Path):
#     '''Allows to add an exposure model to the database. '''

#     with open(assets, 'r') as f:
#         assetcollection = parse_asset_csv(f)
#     with open(exposure, 'r') as e:
#         exposure_params = parse_oq_exposure_file(e)

#     ac_oid = create_asset_collection(exposure_params, assetcollection)

#     typer.echo(f'Created asset collection with ID {ac_oid}.')


# @exposure.command('delete')
# def delete_exposure(asset_collection: int):
#     typer.echo(f'Deleted asset collection {asset_collection}.')


# @exposure.command('list')
# def list_exposure():
#     typer.echo('List of existing asset collections:')


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
