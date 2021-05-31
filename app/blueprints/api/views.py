from flask import Blueprint, jsonify, make_response, request
from app.extensions import csrf
from datamodel import AssetCollection, Asset
from datamodel.base import session
from sqlalchemy import func


api = Blueprint('api', __name__, template_folder='templates')


@api.route('/')
def index():
    return 'Hello World'


@api.get('/test')
def test():
    data = [{'id': 1, '_oid': 1, 'name': 'clara', 'address': 'abc weg 3', 'plz': '1234'},
            {'id': 2, '_oid': 2, 'name': 'peter', 'address': 'abd weg 2', 'plz': '1244'}]
    return make_response(jsonify(data), 200)


@api.post('/exposure')
@csrf.exempt
def post_exposure():
    print('GOT SOMETHING')
    file = request.files.get('exposureJSON')
    print(file)

    return make_response(jsonify({'success': True}), 200)


@api.get('/exposure')
@csrf.exempt
def get_exposure():
    import time

    start = time.time()
    ac = session.query(AssetCollection).all()
    print(time.time() - start)

    start = time.time()
    ac = session.query(AssetCollection, func.count(Asset._oid)).outerjoin(
        Asset).group_by(AssetCollection._oid).all()
    print(time.time() - start)

    response = []
    for coll in ac:
        c = {
            'id': coll[0]._oid,
            'name': coll[0].name,
            'category': coll[0].category,
            'taxonomySource': coll[0].taxonomySource,
            'costTypes': coll[0].costTypes,
            'tagNames': coll[0].tagNames,
            'nAssets': coll[1]
        }
        response.append(c)

    return make_response(jsonify(response), 200)
