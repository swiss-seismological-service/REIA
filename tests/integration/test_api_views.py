from sqlalchemy.sql.expression import distinct
from sqlalchemy.sql.functions import func
from datamodel.asset import Asset, AssetCollection
from io import BytesIO

import json


def test_exposure(client, db_session):
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

    response_get = client.get('/api/v1/exposure')
    assert response_get.status == '200 OK'
    res_json = response_get.json[0]

    assert res_json['name'] == 'Exposure_name'
    assert res_json['nSites'] == 7
