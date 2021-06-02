from flask import Blueprint, jsonify, make_response, request
from app.extensions import csrf
from datamodel import (AssetCollection, Asset, Site,
                       VulnerabilityFunction, VulnerabilityModel,
                       LossModel, LossCalculation)
from datamodel.base import session, engine
from sqlalchemy import func, distinct


import xml.etree.ElementTree as ET
import pandas as pd
import json

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
