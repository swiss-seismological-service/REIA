
from config import get_config
from app import create_app

from unittest import mock
import pytest

import os
from sqlalchemy_utils import database_exists, create_database, drop_database


@pytest.fixture(autouse=True, scope='session')
def mock_settings_env_vars():
    """ set environment variables to testing """
    with mock.patch.dict(os.environ, {'CONFIG_TYPE': 'config.TestingConfig',
                                      'FLASK_ENV': 'development'}):
        yield
