from flask import Blueprint, jsonify, make_response, request
from app.extensions import csrf
from datamodel import AssetCollection
from datamodel.base import session


api = Blueprint('api', __name__, template_folder='templates')


@api.route('/')
def index():
    return 'Hello World'


@api.get('/test')
def test():
    data = [{'name': 'clara'}, {'name': 'peter'}]
    return make_response(jsonify(data), 200)


@api.route('/exposures', methods=['POST'])
@csrf.exempt
def read_exposure():
    print('GOT SOMETHING')
    file = request.files.get('file0')
    print(file)
    print(request.form.get('data'))

    return make_response(jsonify({'success': True}), 200)
