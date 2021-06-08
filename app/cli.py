from app import app
from datamodel import *
from datamodel.base import init_db, drop_db, session, engine
import requests


@ app.cli.group()
def db():
    """Database Commands"""
    pass


@ db.command()
def drop():
    """Drop connected database"""
    drop_db()
    return 'Database dropped'


@ db.command()
def init():
    """Initiate specified database"""
    init_db()
    return 'Database successfully initiated'


@ app.cli.group()
def oqapi():
    """call OQ API Commands"""
    pass


@ oqapi.command()
def list():
    response = requests.get('http://localhost:8800/v1/calc/list')
    print(response.text)
