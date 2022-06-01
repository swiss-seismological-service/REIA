import os


class Config(object):
    """ Base Configuration """
    SECRET_KEY = os.getenv('SECRET_KEY', default='')


class DevelopmentConfig(Config):
    """ Development specific configurations """
    DB_CONNECTION_STRING = 'postgresql+psycopg2://postgres:' \
        'password@localhost:5432/esloss_db'
    OQ_SETTINGS = 'settings/oq_settings.ini'


class ProductionConfig(Config):
    """ Production specific configurations """
    DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING', default='')
