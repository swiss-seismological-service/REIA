from app import app
from datamodel import *
from datamodel.base import init_db, drop_db, session, engine
import requests
import io

import pandas as pd

BPTH = '/home/nicolas/workspaces/SED/ebr/model/'

INPUT = {
    "category": "buildings",
    "name": "Exposure_1km_v03a_CH_modalAmpl_amplAvg",
    "taxonomySource": "SPG (EPFL)",
    "publicId": "id123",
    "id": "uniqueID",
    "tags": [
        "Canton",
        "CantonGemeinde",
        "CantonGemeindePC"
    ],
    "costTypes": [
        "structural"
    ]
}

PREPARE = {
    "description": "prepare risk calculation",
    "aggregate_by": None
}


def createFP(template_name, **kwargs):
    sio = io.StringIO()
    template = app.jinja_env.get_template(template_name)
    template.stream(**kwargs).dump(sio)
    sio.seek(0)
    sio.name = template_name
    return sio


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
def run2():
    bpth = '/home/nicolas/workspaces/SED/ebr/model/'
    test = session.query(AssetCollection).first()
    file1 = createFP('prepare_risk.ini', data=PREPARE)
    # print(file1.read())
    file2 = createFP('exposure.xml', data=test.__dict__)
    # print(file2.read())
    files = {'job_config': file1,
             'input_model_1': file2,
             'input_model_2': open(bpth + 'exposure_assets.csv'),
             'input_model_3': open(bpth + 'structural_vulnerability.xml')}

    response = requests.post(
        'http://localhost:8800/v1/calc/run', files=files)

    if response.ok:
        print("Upload completed successfully!")
        print(response.text)
    else:
        print("Something went wrong!")
        print(response.text)


@ oqapi.command()
def run():
    bpth = '/mnt/c/workspaces/SED/files-event-specific-loss' \
        '/oq_calculations/test_calculation/'

    files1 = {
        'job_config': open(bpth + 'prepare_job_mmi.ini', 'rb'),
        'input_model_1': open(bpth + 'Exposure_dummy2.xml', 'rb'),
        'input_model_2': open(bpth + 'Exposure_dummy2.csv', 'rb'),
        'input_model_3': open(bpth + 'structural_vulnerability_'
                              'model_real_MMI_shift.xml', 'rb')
    }

    files2 = {
        'job_config': open(bpth + 'risk.ini', 'rb'),
        'input_model_1': open(bpth + 'shakemap_files/grid.zip', 'rb')
    }

    response = requests.post(
        'http://localhost:8800/v1/calc/run', files=files1)

    response = requests.post(
        'http://localhost:8800/v1/calc/run', files=files2,
        data={'hazard_job_id': -1})

    if response.ok:
        print("Upload completed successfully!")
        print(response.text)
    else:
        print("Something went wrong!")
        print(response.text)


@ oqapi.command()
def run_python():
    from openquake.commonlib import logs
    from openquake.calculators.base import calculators
    h_id = 0

    with logs.init(
        'job', '/mnt/c/workspaces/SED/files-event-specific-loss/'
            'oq_calculations/test_calculation/prepare_job_mmi.ini') as log:
        calc = calculators(log.get_oqparam(), log.calc_id)
        calc.run()  # run the calculator
        h_id = log.calc_id

    with logs.init(
        'job', '/mnt/c/workspaces/SED/files-event-specific-loss/'
            'oq_calculations/test_calculation/job.ini', hc_id=h_id) as log:
        calc = calculators(log.get_oqparam(), log.calc_id)
        calc.run()  # run the calculator


@ app.cli.group()
def read():
    """read model"""
    pass


@read.command()
def ac():
    import json
    # TODO: read exposure.json into AssetCollection
    with open(BPTH + 'exposure.json') as json_file:
        data = json.load(json_file)
    print(data)

    assetCollection = AssetCollection(**data)
    session.add(assetCollection)
    session.commit()


@ read.command()
def exposure():
    ac_id = 1
    # read into dataframe and rename columns to fit datamodel
    df = pd.read_csv(BPTH + 'exposure_assets.csv', index_col='id')
    df = df.rename(columns={'taxonomy': 'taxonomy_concept',
                            'number': 'buildingCount',
                            'contents': 'contentvalue_value',
                            'day': 'occupancydaytime_value',
                            'structural': 'structuralvalue_value'})
    df['_assetCollection_oid'] = ac_id

    # group by sites
    dg = df.groupby(['lon', 'lat'])
    all_sites = []

    # create site models
    for name, _ in dg:
        site = Site(longitude_value=name[0],
                    latitude_value=name[1],
                    _assetCollection_oid=ac_id)
        session.add(site)
        all_sites.append(site)
    # flush sites to get an ID but keep fast accessible in memory
    session.flush()

    # assign ID back to dataframe using group index
    df['GN'] = dg.grouper.group_info[0]
    df['_site_oid'] = df.apply(lambda x: all_sites[x['GN']]._oid, axis=1)

    # commit so that FK exists in databse
    session.commit()

    # write selected columns directly to database
    df.loc[:, ['taxonomy_concept',
               'buildingCount',
               'contentvalue_value',
               'occupancydaytime_value',
               'structuralvalue_value',
               '_assetCollection_oid',
               '_site_oid']] \
        .to_sql('loss_asset', engine, if_exists='append', index=False)

    pass


@ read.command()
def vulnerability():
    # TODO: read vulnerability xml to VulnerabilityModel

    # TODO: read vulnerability xml to VulnerabilityFunctions
    test = session.query(AssetCollection).first()
    file1 = createFP('exposure.xml', data=test.__dict__)
    print(file1.read())
    pass
