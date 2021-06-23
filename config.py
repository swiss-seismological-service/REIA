import os
from dotenv import load_dotenv
from werkzeug.utils import import_string

load_dotenv()  # load environment variables


def get_config():
    """ Util function to load the correct config from env variable """
    return import_string(os.getenv('CONFIG_TYPE', default='config.ProductionConfig'))


class Config(object):
    """ Base Configuration """
    SERVER_NAME = 'localhost:5000'
    SECRET_KEY = os.getenv('SECRET_KEY', default='')
    CELERY_BROKER_URL = os.getenv('CELERY_BROKER_URL', default='')
    RESULT_BACKEND = os.getenv('RESULT_BACKEND', default='')
    APP_ROOT = os.path.dirname(os.path.abspath(__file__))


class DevelopmentConfig(Config):
    """ Development specific configurations """
    TESTING = False
    DB_CONNECTION_STRING = 'postgresql+psycopg2://postgres:' \
        'postgres@localhost:5432/ebrdb'


class TestingConfig(Config):
    """ Testing specific configurations """
    TESTING = True
    DB_CONNECTION_STRING = 'postgresql+psycopg2://postgres:' \
        'postgres@localhost:5432/test'


class ProductionConfig(Config):
    """ Production specific configurations """
    TESTING = False
    DB_CONNECTION_STRING = os.getenv('PRODUCTION_DB', default='')
