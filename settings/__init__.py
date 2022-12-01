import os

from dotenv import load_dotenv

from reia.utils import import_string

load_dotenv()  # load environment variables


def get_config():
    """ Util function to load the correct config from env variable """
    return import_string(
        "settings."
        f"{os.getenv('CONFIG_TYPE', default='config.ProductionConfig')}")
