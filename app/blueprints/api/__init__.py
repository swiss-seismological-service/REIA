from flask import Blueprint

api = Blueprint('api', __name__, template_folder='templates',
                url_prefix='/api/v1')

from . import views  # noqa
