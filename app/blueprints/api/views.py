
from datamodel.asset import PostalCode
from flask import jsonify, make_response, request, current_app
from sqlalchemy import func, distinct

import pandas as pd
import json
import io
import requests
import time
from datetime import datetime
import threading

import xml.etree.ElementTree as ET
from openquake.calculators.extract import Extractor

from . import api
from app.extensions import csrf
from app.extensions.celery_tasks import test

from datamodel import (session, engine, AssetCollection, Asset, Site,
                       VulnerabilityFunction, VulnerabilityModel, LossConfig,
                       LossModel, LossCalculation, MeanAssetLoss, Municipality)

from .utils import read_asset_csv, sites_from_assets


# @api.route('/')
# def index():
#     test.apply_async()
#     return 'Hello World'


@api.post('/exposure')
@csrf.exempt
def post_exposure():
    # create asset collection from json file
    file_ac = request.files.get('exposureJSON')
    data = json.load(file_ac)
    assetcollection = AssetCollection(**data)

    # read assets into pandas dataframe and rename columns
    file_assets = request.files.get('exposureCSV')
    assets_df = read_asset_csv(file_assets)

    # add tags to session and tag names to Asset Collection
    assetcollection.tagnames = []
    if '_municipality_oid' in assets_df:
        assetcollection.tagnames.append('municipality')
        for el in assets_df['_municipality_oid'].unique():
            session.merge(Municipality(_oid=el))
    if '_postalcode_oid' in assets_df:
        assetcollection.tagnames.append('postalcode')
        for el in assets_df['_postalcode_oid'].unique():
            session.merge(PostalCode(_oid=el))

    # flush assetcollection to get id
    session.add(assetcollection)
    session.flush()

    # assign assetcollection
    assets_df['_assetcollection_oid'] = assetcollection._oid

    # create sites and assign sites list index to assets
    sites, assets_df['sites_list_index'] = sites_from_assets(
        assets_df)

    # add and flush sites to get an ID but keep fast accessible in memory
    session.add_all(sites)
    session.flush()

    # assign ID back to dataframe using group index
    assets_df['_site_oid'] = assets_df.apply(
        lambda x: sites[x['sites_list_index']]._oid, axis=1)

    # commit so that FK exists in databse
    session.commit()

    # write selected columns directly to database
    assets_df.filter(Asset.get_keys()).to_sql(
        'loss_asset', engine, if_exists='append', index=False)

    return get_exposure()


@api.get('/exposure')
@csrf.exempt
def get_exposure():
    # query exposure models and number of Assets and Sites
    asset_collection = session \
        .query(AssetCollection, func.count(distinct(Asset._oid)),
               func.count(distinct(Site._oid))) \
        .select_from(AssetCollection) \
        .outerjoin(Asset) \
        .outerjoin(Site).group_by(AssetCollection._oid).all()

    # assemble response object
    response = []
    for collection in asset_collection:
        collection_dict = collection[0]._asdict()
        collection_dict['assets_count'] = collection[1]
        collection_dict['sites_count'] = collection[2]

        response.append(collection_dict)

    return make_response(jsonify(response), 200)


@api.get('/vulnerability')
@csrf.exempt
def get_vulnerability():
    # query vulnerability Models and number of functions
    vulnerability_model = session.query(VulnerabilityModel,
                                        func.count(VulnerabilityFunction._oid)) \
        .outerjoin(VulnerabilityFunction) \
        .group_by(VulnerabilityModel._oid).all()

    # assemble response object
    response = []
    for model in vulnerability_model:
        model_dict = model[0]._asdict()
        model_dict['functions_count'] = model[1]

        response.append(model_dict)

    return make_response(jsonify(response), 200)


@api.post('/vulnerability')
@csrf.exempt
def post_vulnerability():

    model = {}
    functions = []

    # read xml file with ElementTree
    file = request.files.get('vulnerabilitymodel')
    tree = ET.iterparse(file)

    # strip namespace for easier querying
    for _, el in tree:
        _, _, el.tag = el.tag.rpartition('}')

    root = tree.root

    # read values for VulnerabilityModel
    for child in root.getchildren():
        model['assetcategory'] = child.attrib['assetcategory']
        model['losscategory'] = child.attrib['losscategory']
    model['description'] = root.find('vulnerabilitymodel/description').text

    # read values for VulnerabilityFunctions
    for vF in root.findall('vulnerabilitymodel/vulnerabilityFunction'):
        fun = {}
        fun['taxonomy_concept'] = vF.attrib['id']
        fun['distribution'] = vF.attrib['dist']
        fun['intensitymeasuretype'] = vF.find('imls').attrib['imt']
        fun['intensitymeasurelevels'] = vF.find('imls').text.split(' ')
        fun['meanlossratios'] = vF.find('meanLRs').text.split(' ')
        fun['covariancelossratios'] = vF.find('covLRs').text.split(' ')
        functions.append(fun)

    # assemble vulnerability Model
    vulnerabilitymodel = VulnerabilityModel(**model)
    session.add(vulnerabilitymodel)
    session.flush()

    # assemble vulnerability Functions
    for vF in functions:
        f = VulnerabilityFunction(**vF)
        f._vulnerabilitymodel_oid = vulnerabilitymodel._oid
        session.add(f)

    session.commit()

    return get_vulnerability()


@api.get('/lossmodel')
@csrf.exempt
def get_loss_model():
    # query loss models and count related loss calculations
    loss_models = session.query(LossModel,
                                func.count(LossCalculation._oid)) \
        .outerjoin(LossCalculation) \
        .group_by(LossModel._oid).all()

    # assemble response object
    response = []
    for model in loss_models:
        model_dict = model[0]._asdict()
        model_dict['calculations_count'] = model[1]
        model_dict['_vulnerabilitymodels_oids'] = ','.join(
            [str(v._oid) for v in model[0].vulnerabilitymodels])

        response.append(model_dict)

    return make_response(jsonify(response), 200)


@api.post('/lossmodel')
@csrf.exempt
def post_loss_model():
    # read form data and uploaded file
    form_data = request.form
    file = request.files.get('lossmodel')
    data = json.load(file)

    # append asset collection id
    data['_assetcollection_oid'] = int(form_data['assetcollection'])

    # get related vulnerability models and append
    vmIDs = [
        int(x) for x in form_data['vulnerabilitymodels'].split(',')]
    vulnerabilitymodels = session.query(VulnerabilityModel).filter(
        VulnerabilityModel._oid.in_(vmIDs)).all()
    data['vulnerabilitymodels'] = vulnerabilitymodels

    # assemble LossModel object
    loss_model = LossModel(**data)
    session.add(loss_model)
    session.commit()

    return get_loss_model()


@api.get('/lossconfig')
@csrf.exempt
def get_loss_config():
    loss_config = session.query(LossConfig).all()

    response = []
    for config in loss_config:
        config_dict = config._asdict()

        response.append(config_dict)

    return make_response(jsonify(response), 200)


@api.post('/lossconfig')
@csrf.exempt
def post_loss_config():
    data = request.get_json()
    loss_config = LossConfig(**data)
    session.add(loss_config)
    session.commit()
    return get_loss_config()


@api.post('/calculation/run')
@csrf.exempt
def post_calculation_run():
    # curl -X post http://localhost:5000/calculation/run --header "Content-Type: application/json" --data '{"shakemap":"model/shapefiles.zip"}'

    # get data from database
    lossConfig = session.query(LossConfig).get(1)

    lossmodel = session.query(LossModel).get(lossConfig._lossmodel_oid)
    exposureModel = session.query(AssetCollection).get(
        lossmodel._assetcollection_oid)

    vulnerabilitymodel = session.query(VulnerabilityModel) \
        .join(LossModel, VulnerabilityModel.lossmodels). \
        filter(VulnerabilityModel.losscategory == lossConfig.losscategory)\
        .first()

    # create in memory files
    # exposure.xml
    exposure_xml = createFP('api/exposure.xml', data=exposureModel.to_dict())

    # exposure_assets.csv
    assets = pd.DataFrame([x.to_dict()
                           for x in exposureModel.assets]).set_index('id')
    exposure_assets_csv = io.StringIO()
    assets.to_csv(exposure_assets_csv)
    exposure_assets_csv.seek(0)
    exposure_assets_csv.name = 'exposure_assets.csv'

    # vulnerability.xml
    vulnerability_xml = createFP(
        'api/vulnerability.xml', data=vulnerabilitymodel.to_dict())

    loss_config_dict = lossConfig.to_dict()

    # pre-calculation.ini
    prepare_risk_ini = createFP('api/prepare_risk.ini', data=loss_config_dict)

    # risk.ini
    risk_ini = createFP('api/risk.ini', data=loss_config_dict)

    # TODO: get shakemap
    shakemap_address = request.get_json()['shakemap']
    shakemap_zip = open(shakemap_address, 'rb')

    # send files to calculation endpoint
    files = {'job_config': prepare_risk_ini,
             'input_model_1': exposure_xml,
             'input_model_2': exposure_assets_csv,
             'input_model_3': vulnerability_xml}

    response = requests.post(
        'http://localhost:8800/v1/calc/run', files=files)

    if response.ok:
        print("Upload completed successfully!")
        pre_job_id = response.json()['job_id']
    else:
        print("Something went wrong!")
        print(response.text)
        return make_response(jsonify({}), 200)

    # wait for pre-calculation to finish
    while requests.get(f'http://localhost:8800/v1/calc/{pre_job_id}/status')\
            .json()['status'] not in ['complete', 'failed']:
        time.sleep(1)
    response = requests.get(
        f'http://localhost:8800/v1/calc/{pre_job_id}/status')

    if response.json()['status'] != 'complete':
        return make_response(response.json(), 200)

    # send files to calculation endpoint
    files2 = {
        'job_config': risk_ini,
        'input_model_1': shakemap_zip
    }

    response = requests.post(
        'http://localhost:8800/v1/calc/run', files=files2,
        data={'hazard_job_id': pre_job_id})

    if response.ok:
        print("Upload completed successfully!")
    else:
        print("Something went wrong!")
        print(response.text)

    losscalculation = LossCalculation(
        shakemapid_resourceid='shakemap_address',
        _lossmodel_oid=lossmodel._oid,
        losscategory=lossConfig.losscategory,
        aggregateBy=lossConfig.aggregateBy,
        timestamp_starttime=datetime.now()
    )
    session.add(losscalculation)
    session.commit()

    # wait, fetch and save results
    thread = threading.Thread(target=waitAndFetchResults(
        response.json()['job_id'], losscalculation._oid))
    thread.daemon = True
    thread.start()
    return make_response(response.json(), 200)


@api.get('/losscalculation')
@csrf.exempt
def get_loss_calculation():
    response = []
    loss_calculation = session.query(LossCalculation).all()

    for calculation in loss_calculation:
        calculation_dict = calculation._asdict()

        response.append(calculation_dict)

    return make_response(jsonify(response), 200)


def waitAndFetchResults(oqJobId, calcId):
    # wait for calculation to finish
    while requests.get(f'http://localhost:8800/v1/calc/{oqJobId}/status')\
            .json()['status'] not in ['complete', 'failed']:
        time.sleep(1)
    response = requests.get(
        f'http://localhost:8800/v1/calc/{oqJobId}/status')

    if response.json()['status'] != 'complete':
        return None
    # fetch results
    extractor = Extractor(oqJobId)
    data = extractor.get('avg_losses-rlzs').to_dframe()

    data = data[['asset_id', 'value']].rename(
        columns={'asset_id': '_asset_oid', 'value': 'loss_value'})

    # save results to database
    data = data.apply(lambda x: MeanAssetLoss(
        _losscalculation_oid=calcId, **x), axis=1)
    session.add_all(data)
    session.commit()
    print('Done saving results')


def createFP(template_name, **kwargs):
    """ create file pointer """
    sio = io.StringIO()
    template = current_app.jinja_env.get_template(template_name)
    template.stream(**kwargs).dump(sio)
    sio.seek(0)
    sio.name = template_name.rsplit('/', 1)[-1]
    return sio
