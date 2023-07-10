
import typer

from reia.datamodel.base import drop_db, init_db

app = typer.Typer(add_completion=False)
db = typer.Typer()

app.add_typer(db, name='db',
              help='Database Commands')


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
