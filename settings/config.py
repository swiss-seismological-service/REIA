import os


class Config(object):
    """ Base Configuration """
    SECRET_KEY = os.getenv('SECRET_KEY', default='')
    OQ_SETTINGS = [(0.3, 'settings/oq_settings.ini'),
                   (0.7, 'settings/oq_settings_sion.ini')]


class DevelopmentConfig(Config):
    """ Development specific configurations """
    DB_CONNECTION_STRING = 'postgresql+psycopg2://postgres:' \
        'password@localhost:5432/esloss_db'
    OQ_API_SERVER = 'http://localhost:8800'


class ProductionConfig(Config):
    """ Production specific configurations """
    DB_CONNECTION_STRING = os.getenv('DB_CONNECTION_STRING', default='')
