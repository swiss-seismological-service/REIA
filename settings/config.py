import os


class Config(object):
    """ Base Configuration """
    OQ_API_SERVER = os.getenv('OQ_HOST')
    OQ_API_AUTH = dict(
        username=os.getenv('OQ_USER'),
        password=os.getenv('OQ_PASSWORD'))
    DB_CONNECTION_STRING = \
        f"postgresql+psycopg2://{os.getenv('DB_USER')}:" \
        f"{os.getenv('DB_PASSWORD')}@{os.getenv('POSTGRES_HOST')}" \
        f":{os.getenv('POSTGRES_PORT')}/{os.getenv('DB_NAME')}"


class DevelopmentConfig(Config):
    """ Development specific configurations """
    pass


class ProductionConfig(Config):
    """ Production specific configurations """
    pass
