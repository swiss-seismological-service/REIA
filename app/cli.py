from app import app
import click
from datamodel import *
from datamodel.base import init_db, drop_db, session, engine
import requests
from openquake.calculators.extract import Extractor
from openquake.commonlib.datastore import read
import pandas
import time


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


@oqapi.command()
@click.argument('type')
def extract(type):
    extractor = Extractor(520)
    data = extractor.get(type).to_dframe()

    data = data[['asset_id', 'value']].rename(
        columns={'asset_id': '_asset_oid', 'value': 'loss_value'})

    data = data.apply(lambda x: MeanAssetLoss(
        _lossCalculation_oid=2, **x), axis=1)
    session.add_all(data)
    session.commit()
    pass


@oqapi.command()
def readit():
    dstore = read(520)
    print([key for key in dstore])
    pass
