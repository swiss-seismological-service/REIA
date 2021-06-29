from flask.json import jsonify

from datamodel.lossmodel import LossConfig, LossModel
from datamodel.vulnerability import VulnerabilityModel
from datamodel.asset import Asset, AssetCollection, Site
from io import BytesIO


def test_post_exposure(client, db_session):
    with open('tests/data/exposure.json', 'rb') as file:
        exposure_fp = BytesIO(file.read())

    with open('tests/data/exposure_assets.csv', 'rb') as file:
        assets_fp = BytesIO(file.read())

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


def test_post_vulnerability(client, db_session):
    with open('tests/data/structural_vulnerability.xml', 'rb') as file:
        vulnerability_fp = BytesIO(file.read())

    response = client.post('/api/v1/vulnerability', content_type='multipart/form-data',
                           data={'vulnerabilitymodel': (vulnerability_fp,
                                                        'structural_vulnerability.xml')})

    assert response.status == '200 OK'

    vulnerability_model = db_session.query(VulnerabilityModel).first()
    assert vulnerability_model.description == 'Macroseismic Intensity Based ' \
        'Vulnerability Model (structural)'

    vulnerability_functions = vulnerability_model.vulnerabilityfunctions

    assert len(vulnerability_functions) == 2
    assert vulnerability_functions[1].intensitymeasuretype == 'MMI'
    assert vulnerability_functions[1].meanlossratios[3] == 0.001593


def test_post_lossmodel(client, db_session):

    vulnerability_model_1 = VulnerabilityModel(losscategory='structural')
    vulnerability_model_2 = VulnerabilityModel(losscategory='structural')
    asset_collection = AssetCollection(name='test_collection')

    db_session.add(vulnerability_model_1)
    db_session.add(vulnerability_model_2)
    db_session.add(asset_collection)
    db_session.commit()

    with open('tests/data/risk.ini', 'rb') as file:
        loss_fp = BytesIO(file.read())

    response = client.post('/api/v1/lossmodel', content_type='multipart/form-data',
                           data={'riskini': (loss_fp, 'risk.ini'),
                                 '_assetcollection_oid': 1,
                                 '_vulnerabilitymodels_oids': '1,2'})

    assert response.status == '200 OK'

    loss_model = db_session.query(LossModel).first()
    assert loss_model.description == 'Test Scenario for Shakemap from SED'
    assert loss_model.masterseed == 23
    assert loss_model.vulnerabilitymodels[0] == vulnerability_model_1 or \
        loss_model.vulnerabilitymodels[0] == vulnerability_model_2


def test_post_lossconfig(client, db_session):
    asset_collection = AssetCollection(name='test_collection')
    loss_model = LossModel(preparationcalculationmode='scenario', maincalculationmode='scenario_risk',
                           numberofgroundmotionfields=100, assetcollection=asset_collection)

    db_session.add(loss_model)
    db_session.commit()
    response = client.post('/api/v1/lossconfig', content_type='application/json',
                           json={'losscategory': 'structural',
                                 'aggregateby': 'postalcode',
                                 '_lossmodel_oid': 1})

    assert response.status == '200 OK'

    loss_config = db_session.query(LossConfig).first()
    assert loss_config.losscategory == 'structural'
    assert loss_config.aggregateby == 'postalcode'
    assert loss_config.lossmodel == loss_model
