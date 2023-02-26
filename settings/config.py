import os


class Config(object):
    """ Base Configuration """
    SECRET_KEY = os.getenv('SECRET_KEY', default='')
    OQ_SETTINGS = [(0.3, 'settings/oq_settings.ini'),
                   (0.7, 'settings/oq_settings_sion.ini')]
    OQ_API_SERVER = 'http://localhost:8800'
    SCENARIO_DATA_FOLDER = os.getenv('SCENARIO_DATA_FOLDER')
    OQ_API_AUTH = dict(
        username=os.getenv('OQ_USER'),
        password=os.getenv('OQ_PASSWORD'))


class DevelopmentConfig(Config):
    """ Development specific configurations """
    DB_CONNECTION_STRING = 'postgresql+psycopg2://postgres:' \
        'password@localhost:5432/esloss_db'


class ProductionConfig(Config):
    """ Production specific configurations """
    DB_CONNECTION_STRING = \
        f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:" \
        f"{os.getenv('POSTGRES_PASSWORD')}@{os.getenv('POSTGRES_HOST')}" \
        f":{os.getenv('POSTGRES_PORT')}/{os.getenv('POSTGRES_DB')}"
    OQ_API_SERVER = os.getenv('OQ_HOST')
