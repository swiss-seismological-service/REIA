import os
from dotenv import load_dotenv
from core.utils import import_string

load_dotenv()  # load environment variables


def get_config():
    """ Util function to load the correct config from env variable """
    return import_string(
        os.getenv(
            'CONFIG_TYPE',
            default='config.ProductionConfig'))


class Config(object):
    """ Base Configuration """
    SECRET_KEY = os.getenv('SECRET_KEY', default='')


class DevelopmentConfig(Config):
    """ Development specific configurations """
    DB_CONNECTION_STRING = 'postgresql+psycopg2://postgres:' \
        'password@localhost:5432/esloss_db'


class ProductionConfig(Config):
    """ Production specific configurations """
    DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING', default='')
