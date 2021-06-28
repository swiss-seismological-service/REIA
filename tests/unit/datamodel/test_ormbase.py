from datamodel import AssetCollection

import json


def test_asdict(db_session):
    """
    Test ORMBase's _asdict() method.
    """
    with open('tests/data/exposure.json') as f:
        data = json.load(f)
    ac = AssetCollection(**data)
    target_dict = {'_oid': None,
                   'creationinfo_author': None,
                   'creationinfo_authoruri_resourceid': None,
                   'creationinfo_agencyid': None,
                   'creationinfo_agencyuri_resourceid': None,
                   'creationinfo_creationtime': None, 'creationinfo_version': None,
                   'creationinfo_copyrightowner': None,
                   'creationinfo_copyrightowneruri_resourceid': None,
                   'creationinfo_license': None,
                   'publicid_resourceid': 'id123',
                   'name': 'Exposure_name',
                   'category': 'buildings',
                   'taxonomysource': 'SPG (EPFL)',
                   'costtypes': ['structural'],
                   'tagnames': ['canton', 'municipality', 'postalcode']}

    assert ac._asdict() == target_dict
