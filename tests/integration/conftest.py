from app import create_app
from datamodel.lossmodel import LossCalculation, LossConfig, LossModel
from datamodel.vulnerability import VulnerabilityFunction, VulnerabilityModel
from datamodel.asset import AssetCollection, Site, Asset

import pytest
from datetime import datetime


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
