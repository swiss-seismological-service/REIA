from datamodel import AssetCollection, Asset
import json


def test_asset_collection():
    with open('tests/data/exposure.json') as f:
        data = json.load(f)
    ac = AssetCollection(**data)
    data['id'] = None
    assert ac.to_dict() == data
