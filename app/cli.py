from app import app
from datamodel import *
from datamodel.base import init_db, drop_db


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
