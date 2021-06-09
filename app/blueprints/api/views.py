from datamodel.lossmodel import LossConfig
from flask import Blueprint, jsonify, make_response, request, current_app
from app.extensions import csrf
from datamodel import (AssetCollection, Asset, Site,
                       VulnerabilityFunction, VulnerabilityModel,
                       LossModel, LossCalculation)
from datamodel.base import session, engine
from sqlalchemy import func, distinct

import xml.etree.ElementTree as ET
import pandas as pd
import json
import io
import requests
import time
from datetime import datetime

api = Blueprint('api', __name__, template_folder='templates')


@api.route('/')
def index():
    return 'Hello World'


@api.post('/exposure')
@csrf.exempt
def post_exposure():
    # create asset collection from json file
    file_ac = request.files.get('exposureJSON')
    data = json.load(file_ac)
    assetCollection = AssetCollection(**data)
    session.add(assetCollection)
    session.flush()

    # read assets into pandas dataframe and rename columns
    file_assets = request.files.get('exposureCSV')
    df = pd.read_csv(file_assets, index_col='id')

    df = df.rename(columns={'taxonomy': 'taxonomy_concept',
                            'number': 'buildingCount',
                            'contents': 'contentvalue_value',
                            'day': 'occupancydaytime_value',
                            'structural': 'structuralvalue_value'})
    df['_assetCollection_oid'] = assetCollection._oid

    # group by sites
    dg = df.groupby(['lon', 'lat'])
    all_sites = []

    # create site models
    for name, _ in dg:
        site = Site(longitude_value=name[0],
                    latitude_value=name[1],
                    _assetCollection_oid=assetCollection._oid)
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

    return get_exposure()


@api.get('/exposure')
@csrf.exempt
def get_exposure():
    # query exposure models and number of Assets and Sites
    ac = session \
        .query(AssetCollection, func.count(distinct(Asset._oid)),
               func.count(distinct(Site._oid))) \
        .select_from(AssetCollection) \
        .outerjoin(Asset) \
        .outerjoin(Site).group_by(AssetCollection._oid).all()

    # assemble response object
    response = []
    for coll in ac:
        c = {
            'id': coll[0]._oid,
            'name': coll[0].name,
            'category': coll[0].category,
            'taxonomySource': coll[0].taxonomySource,
            'costTypes': coll[0].costTypes,
            'tagNames': coll[0].tagNames,
            'nAssets': coll[1],
            'nSites': coll[2]
        }
        response.append(c)

    return make_response(jsonify(response), 200)


@api.get('/vulnerability')
@csrf.exempt
def get_vulnerability():
    # query vulnerability Models and number of functions
    vm = session.query(VulnerabilityModel,
                       func.count(VulnerabilityFunction._oid)) \
        .outerjoin(VulnerabilityFunction) \
        .group_by(VulnerabilityModel._oid).all()

    # assemble response object
    response = []
    for coll in vm:
        c = {
            'id': coll[0]._oid,
            'lossCategory': coll[0].lossCategory,
            'assetCategory': coll[0].assetCategory,
            'description': coll[0].description,
            'nFunctions': coll[1]
        }
        response.append(c)

    return make_response(jsonify(response), 200)


@api.post('/vulnerability')
@csrf.exempt
def post_vulnerability():

    model = {}
    functions = []

    # read xml file with ElementTree
    file = request.files.get('vulnerabilityModel')
    tree = ET.iterparse(file)

    # strip namespace for easier querying
    for _, el in tree:
        _, _, el.tag = el.tag.rpartition('}')

    root = tree.root

    # read values for VulnerabilityModel
    for child in root.getchildren():
        model['assetCategory'] = child.attrib['assetCategory']
        model['lossCategory'] = child.attrib['lossCategory']
    model['description'] = root.find('vulnerabilityModel/description').text

    # read values for VulnerabilityFunctions
    for vF in root.findall('vulnerabilityModel/vulnerabilityFunction'):
        fun = {}
        fun['taxonomy_concept'] = vF.attrib['id']
        fun['distribution'] = vF.attrib['dist']
        fun['intensityMeasureType'] = vF.find('imls').attrib['imt']
        fun['intensityMeasureLevels'] = vF.find('imls').text.split(' ')
        fun['meanLossRatios'] = vF.find('meanLRs').text.split(' ')
        fun['covarianceLossRatios'] = vF.find('covLRs').text.split(' ')
        functions.append(fun)

    # assemble vulnerability Model
    vulnerabilityModel = VulnerabilityModel(**model)
    session.add(vulnerabilityModel)
    session.flush()

    # assemble vulnerability Functions
    for vF in functions:
        f = VulnerabilityFunction(**vF)
        f._vulnerabilityModel_oid = vulnerabilityModel._oid
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
    for loss in loss_models:
        new_model = {
            'id': loss[0]._oid,
            'description': loss[0].description,
            'preparationCalculationMode': loss[0].preparationCalculationMode,
            'mainCalculationMode': loss[0].mainCalculationMode,
            'numberOfGroundMotionFields': loss[0].numberOfGroundMotionFields,
            'maximumDistance': loss[0].maximumDistance,
            'masterSeed': loss[0].masterSeed,
            'randomSeed': loss[0].randomSeed,
            'truncationLevel': loss[0].truncationLevel,
            'vulnerabilityModels':
            ','.join([str(v._oid) for v in loss[0].vulnerabilityModels]),
            'assetCollection': loss[0]._assetCollection_oid,
            'nCalculations': loss[1]
        }
        response.append(new_model)
    return make_response(jsonify(response), 200)


@api.post('/lossmodel')
@csrf.exempt
def post_loss_model():
    # read form data and uploaded file
    form_data = request.form
    file = request.files.get('lossModel')
    data = json.load(file)

    # append asset collection id
    data['_assetCollection_oid'] = int(form_data['assetCollection'])

    # get related vulnerability models and append
    vmIDs = [
        int(x) for x in form_data['vulnerabilityModels'].split(',')]
    vulnerabilityModels = session.query(VulnerabilityModel).filter(
        VulnerabilityModel._oid.in_(vmIDs)).all()
    data['vulnerabilityModels'] = vulnerabilityModels

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
        new_config = {
            'id': config._oid,
            'lossCategory': config.lossCategory,
            'aggregateBy': config.aggregateBy,
            'lossModel': config._lossModel_oid
        }
        response.append(new_config)

    return make_response(jsonify(response), 200)


@api.post('/lossconfig')
@csrf.exempt
def post_loss_config():
    data = request.get_json()
    loss_config = LossConfig(
        lossCategory=data['lossCategory'],
        aggregateBy=data['aggregateBy'],
        _lossModel_oid=data['lossModelId'])
    session.add(loss_config)
    session.commit()
    return get_loss_config()


@api.post('/calculation/run')
@csrf.exempt
def post_calculation_run():
    # curl -X post http://localhost:5000/calculation/run --header "Content-Type: application/json" --data '{"shakemap":"model/shapefiles.zip"}'

    # get data from database
    lossConfig = session.query(LossConfig).get(3)

    lossModel = session.query(LossModel).get(lossConfig._lossModel_oid)
    exposureModel = session.query(AssetCollection).get(
        lossModel._assetCollection_oid)

    vulnerabilityModel = session.query(VulnerabilityModel) \
        .join(LossModel, VulnerabilityModel.lossModels). \
        filter(VulnerabilityModel.lossCategory == lossConfig.lossCategory)\
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
        'api/vulnerability.xml', data=vulnerabilityModel.to_dict())

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

    lossCalculation = LossCalculation(
        shakemapid_resourceid='shakemap_address',
        _lossModel_oid=lossModel._oid,
        lossCategory=lossConfig.lossCategory,
        aggregateBy=lossConfig.aggregateBy,
        timestamp_startTime=datetime.now()
    )
    session.add(lossCalculation)
    session.commit()

    # TODO: wait for calculation to finish

    # TODO: fetch results

    # TODO: save results to database

    return make_response(response.json(), 200)


def createFP(template_name, **kwargs):
    """ create file pointer """
    sio = io.StringIO()
    template = current_app.jinja_env.get_template(template_name)
    template.stream(**kwargs).dump(sio)
    sio.seek(0)
    sio.name = template_name.rsplit('/', 1)[-1]
    return sio
