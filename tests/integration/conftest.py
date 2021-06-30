from io import BytesIO
from app import create_app
from datamodel.lossmodel import LossCalculation, LossConfig, LossModel
from datamodel.vulnerability import VulnerabilityFunction, VulnerabilityModel
from datamodel.asset import AssetCollection, Site, Asset

import pytest
from datetime import datetime


@pytest.fixture(scope='class')
def test_query(db_class):
    obj = {}
    # get data from database
    obj['loss_config'] = db_class.query(LossConfig).get(1)
    obj['loss_model'] = obj['loss_config'].lossmodel
    obj['asset_collection'] = obj['loss_model'].assetcollection
    obj['vulnerability_model'] = db_class.query(VulnerabilityModel) \
        .join(LossModel, VulnerabilityModel.lossmodels). \
        filter(VulnerabilityModel.losscategory == obj['loss_config'].losscategory)\
        .first()
    yield obj


@pytest.fixture(scope='class')
def test_data_from_files(client_class):

    with open('tests/integration/data/exposure.xml', 'rb') as file:
        exposure_fp = BytesIO(file.read())

    with open('tests/integration/data/exposure_assets.csv', 'rb') as file:
        assets_fp = BytesIO(file.read())

    with open('tests/integration/data/structural_vulnerability.xml', 'rb') as file:
        vulnerability_fp = BytesIO(file.read())

    with open('tests/integration/data/risk.ini', 'rb') as file:
        loss_fp = BytesIO(file.read())

    client_class.post('/api/v1/assetcollection', content_type='multipart/form-data',
                      data={'exposureCSV': (assets_fp, 'exposure_assets.csv'),
                            'exposureXML': (exposure_fp, 'exposure.xml')})

    client_class.post('/api/v1/vulnerabilitymodel', content_type='multipart/form-data',
                      data={'vulnerabilitymodel': (vulnerability_fp,
                                                   'structural_vulnerability.xml')})

    client_class.post('/api/v1/lossmodel', content_type='multipart/form-data',
                      data={'riskini': (loss_fp, 'risk.ini'),
                            '_assetcollection_oid': 1,
                            '_vulnerabilitymodels_oids': '1'})

    client_class.post('/api/v1/lossconfig', content_type='application/json',
                      json={'losscategory': 'structural',
                            'aggregateby': None,
                            '_lossmodel_oid': 1})


@pytest.fixture(scope='class')
def test_data(db_class):
    asset_collection = AssetCollection(name='test_collection')

    site = Site(assetcollection=asset_collection,
                latitude_value=1.1,
                longitude_value=1.2)

    asset = Asset(assetcollection=asset_collection,
                  site=site,
                  buildingcount=1,
                  contentvalue_value=1,
                  structuralvalue_value=1,
                  occupancydaytime_value=1,
                  taxonomy_concept='1')

    vulnerability_model_1 = VulnerabilityModel(losscategory='structural')
    vulnerability_model_2 = VulnerabilityModel(_oid=2, losscategory='content')
    vulnerability_function = VulnerabilityFunction(distribution='beta',
                                                   intensitymeasuretype='mmi',
                                                   vulnerabilitymodel=vulnerability_model_1)

    asset_collection = AssetCollection(name='test_collection')

    loss_model = LossModel(preparationcalculationmode='scenario',
                           maincalculationmode='scenario',
                           numberofgroundmotionfields=200,
                           assetcollection=asset_collection,
                           vulnerabilitymodels=[vulnerability_model_1, vulnerability_model_2])

    loss_config = LossConfig(losscategory='structural', lossmodel=loss_model)

    asset_collection = AssetCollection(name='test_collection')

    loss_calculation = LossCalculation(shakemapid_resourceid='some_id',
                                       lossmodel=loss_model,
                                       losscategory='structural',
                                       timestamp_starttime=datetime.now().isoformat(' ', 'seconds'))

    db_class.add(loss_calculation)
    db_class.add(loss_config)
    db_class.commit()
