from flask import Blueprint

api = Blueprint('api', __name__, template_folder='templates')


@api.route('/')
def index():
    return 'Hello World'
