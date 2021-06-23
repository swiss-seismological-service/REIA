import pytest
from unittest import mock

import os


@pytest.fixture()
def mock_env_production():
    """ set environment variables temporarily to production """
    with mock.patch.dict(os.environ, {'FLASK_ENV': 'production'}):
        yield
