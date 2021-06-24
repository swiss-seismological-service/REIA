from datamodel import AssetCollection, Asset, Site
import json


def test_asset_collection():
    with open('tests/data/exposure.json') as f:
        data = json.load(f)
    ac = AssetCollection(**data)
    data['id'] = None
    assert ac.to_dict() == data


def test_asset():
    assert Asset.get_keys() == Asset.__table__.columns.keys()

    site = Site(longitude_value=10, latitude_value=11)

    asset = Asset(_oid=1, site=site, taxonomy_concept='tax', buildingCount=21,
                  structuralvalue_value=1321, contentvalue_value=1111, occupancydaytime_value=9)

    dct = {'id': 1, 'lon': 10, 'lat': 11, 'taxonomy': 'tax', 'number': 21,
           'structural': 1321, 'contents': 1111, 'day': 9}

    assert asset.to_dict() == dct
