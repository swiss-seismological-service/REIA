from flask import Blueprint, jsonify, make_response, request
from app.extensions import csrf
from datamodel import (AssetCollection, Asset, Site,
                       VulnerabilityFunction, VulnerabilityModel)
from datamodel.base import session, engine
from sqlalchemy import func, distinct


import xml.etree.ElementTree as ET
import pandas as pd
import json

api = Blueprint('api', __name__, template_folder='templates')


@api.route('/')
def index():
    return 'Hello World'


@api.get('/test')
def test():
    data = [{'id': 1, '_oid': 1, 'name': 'clara',
             'address': 'abc weg 3', 'plz': '1234'},
            {'id': 2, '_oid': 2, 'name': 'peter',
            'address': 'abd weg 2', 'plz': '1244'}]
    return make_response(jsonify(data), 200)


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

    ac = session \
        .query(AssetCollection, func.count(distinct(Asset._oid)),
               func.count(distinct(Site._oid))) \
        .select_from(AssetCollection) \
        .outerjoin(Asset) \
        .outerjoin(Site).group_by(AssetCollection._oid).all()

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
    import time

    ac = session.query(VulnerabilityModel,
                       func.count(VulnerabilityFunction._oid)) \
        .outerjoin(VulnerabilityFunction) \
        .group_by(VulnerabilityModel._oid).all()

    response = []

    for coll in ac:
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
    # create asset collection from json file
    file = request.files.get('vulnerabilityModel')

    model = {}
    functions = []

    # with open(file) as xml_file:

    tree = ET.iterparse(file)

    for _, el in tree:
        _, _, el.tag = el.tag.rpartition('}')  # strip ns

    root = tree.root

    # read vulnerability xml to VulnerabilityModel
    for child in root.getchildren():
        model['assetCategory'] = child.attrib['assetCategory']
        model['lossCategory'] = child.attrib['lossCategory']
    model['description'] = root.find('vulnerabilityModel/description').text

    # TODO: read vulnerability xml to VulnerabilityFunctions
    for vF in root.findall('vulnerabilityModel/vulnerabilityFunction'):
        fun = {}
        fun['taxonomy_concept'] = vF.attrib['id']
        fun['distribution'] = vF.attrib['dist']
        fun['intensityMeasureType'] = vF.find('imls').attrib['imt']
        fun['intensityMeasureLevels'] = vF.find('imls').text.split(' ')
        fun['meanLossRatios'] = vF.find('meanLRs').text.split(' ')
        fun['covarianceLossRatios'] = vF.find('covLRs').text.split(' ')
        functions.append(fun)

    vulnerabilityModel = VulnerabilityModel(**model)
    session.add(vulnerabilityModel)
    session.flush()

    for vF in functions:
        f = VulnerabilityFunction(**vF)
        f._vulnerabilityModel_oid = vulnerabilityModel._oid
        session.add(f)

    session.commit()

    return get_vulnerability()
