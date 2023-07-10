import os

import dotenv

from reia.utils import import_string

dotenv_file = dotenv.find_dotenv(usecwd=True)
dotenv.load_dotenv(dotenv_file)


def get_config():
    """ Util function to load the correct config from env variable """
    return import_string(
        "settings."
        f"{os.getenv('CONFIG_TYPE', default='config.ProductionConfig')}")
