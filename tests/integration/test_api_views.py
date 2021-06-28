from datamodel.lossmodel import LossCalculation, LossConfig, LossModel
from datamodel.vulnerability import VulnerabilityFunction, VulnerabilityModel
from sqlalchemy.sql.expression import distinct
from sqlalchemy.sql.functions import func
from datamodel.asset import Asset, AssetCollection, Site
from io import BytesIO
from unittest import TestCase

from datetime import datetime
from dateutil.parser import parse


def test_post_exposure(client, db_session):
    with open('tests/data/exposure.json', 'rb') as ac:
        exposure_fp = BytesIO(ac.read())

    with open('tests/data/exposure_assets.csv', 'rb') as assets:
        assets_fp = BytesIO(assets.read())

    response = client.post('/api/v1/exposure', content_type='multipart/form-data',
                           data={'exposureCSV': (assets_fp, 'exposure_assets.csv'),
                                 'exposureJSON': (exposure_fp, 'exposure.json')})

    assert response.status == '200 OK'

    db_assetcollection = db_session.query(AssetCollection).first()
    assert db_assetcollection.taxonomysource == 'SPG (EPFL)'

    n_assets = db_session.query(Asset).filter(
        Asset._assetcollection_oid == db_assetcollection._oid).count()
    assert n_assets == 10
    n_sites = db_session.query(Site).filter(
        Site._assetcollection_oid == db_assetcollection._oid).count()
    assert n_sites == 7


def test_get_exposure(client, db_session):
    asset_collection = AssetCollection(name='test_collection')

    site = Site(assetcollection=asset_collection,
                latitude_value=1.1, longitude_value=1.2)

    asset = Asset(assetcollection=asset_collection, site=site, buildingcount=1,
                  contentvalue_value=1, structuralvalue_value=1, occupancydaytime_value=1,
                  taxonomy_concept='1')

    db_session.add(asset_collection)
    db_session.commit()

    response_db = db_session.query(AssetCollection).first()._asdict()
    response_db['assets_count'] = db_session.query(Asset).filter(
        Asset._assetcollection_oid == asset_collection._oid).count()
    response_db['sites_count'] = db_session.query(Site).filter(
        Site._assetcollection_oid == asset_collection._oid).count()

    response_get = client.get('/api/v1/exposure')
    assert response_get.status == '200 OK'

    response_json = response_get.json[0]
    response_json['creationinfo_creationtime'] = parse(
        response_json['creationinfo_creationtime'], ignoretz=True)
    assert response_json == response_db


def test_get_vulnerability(client, db_session):
    vulnerability_model = VulnerabilityModel(losscategory='structural')
    vulnerability_function = VulnerabilityFunction(
        distribution='beta', intensitymeasuretype='mmi', vulnerabilitymodel=vulnerability_model)

    db_session.add(vulnerability_model)
    db_session.commit()

    response_db = db_session.query(VulnerabilityModel).first()._asdict()
    response_db['functions_count'] = db_session.query(VulnerabilityFunction).filter(
        VulnerabilityFunction._vulnerabilitymodel_oid == vulnerability_model._oid).count()

    response_get = client.get('/api/v1/vulnerability')
    assert response_get.status == '200 OK'

    response_json = response_get.json[0]
    assert response_json == response_db


def test_get_lossmodel(client, db_session):
    asset_collection = AssetCollection(name='test_collection')
    vulnerability_model1 = VulnerabilityModel(
        _oid=1, losscategory='structural')
    vulnerability_model2 = VulnerabilityModel(_oid=2, losscategory='content')
    loss_model = LossModel(preparationcalculationmode='scenario', maincalculationmode='scenario',
                           numberofgroundmotionfields=200, assetcollection=asset_collection,
                           vulnerabilitymodels=[vulnerability_model1, vulnerability_model2])
    db_session.add(loss_model)
    db_session.commit()

    response_db = db_session.query(LossModel).first()._asdict()
    response_db['_vulnerabilitymodels_oids'] = [2, 1]
    response_db['calculations_count'] = 0

    response_get = client.get('/api/v1/lossmodel')
    assert response_get.status == '200 OK'

    response_json = response_get.json[0]
    TestCase().assertEqual(response_json, response_db)


def test_get_lossconfig(client, db_session):
    asset_collection = AssetCollection(name='test_collection')
    loss_model = LossModel(preparationcalculationmode='scenario', maincalculationmode='scenario',
                           numberofgroundmotionfields=200, assetcollection=asset_collection)
    loss_config = LossConfig(losscategory='structural', lossmodel=loss_model)

    db_session.add(loss_config)
    db_session.commit()

    response_db = db_session.query(LossConfig).first()._asdict()

    response_get = client.get('/api/v1/lossconfig')
    assert response_get.status == '200 OK'

    response_json = response_get.json[0]
    assert response_json == response_db


def test_get_losscalculation(client, db_session):
    asset_collection = AssetCollection(name='test_collection')
    loss_model = LossModel(preparationcalculationmode='scenario', maincalculationmode='scenario',
                           numberofgroundmotionfields=200, assetcollection=asset_collection)
    loss_calculation = LossCalculation(shakemapid_resourceid='some_id', lossmodel=loss_model,
                                       losscategory='structural',
                                       timestamp_starttime=datetime.now().isoformat(' ', 'seconds'))

    db_session.add(loss_calculation)
    db_session.commit()

    response_db = db_session.query(LossCalculation).first()._asdict()

    response_get = client.get('/api/v1/losscalculation')
    assert response_get.status == '200 OK'

    response_json = response_get.json[0]
    response_json['creationinfo_creationtime'] = parse(
        response_json['creationinfo_creationtime'], ignoretz=True)
    response_json['timestamp_starttime'] = parse(
        response_json['timestamp_starttime'], ignoretz=True)
    assert response_json == response_db
