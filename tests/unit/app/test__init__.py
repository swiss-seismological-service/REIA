from flask import current_app
from config import Config

import uuid
import os


def test_logging(mock_env_production, client):
    new_uuid = uuid.uuid4().hex
    current_app.logger.info(new_uuid)

    with open(os.path.join(Config.APP_ROOT, 'logs', 'app.log')) as f:
        for line in f:
            pass
        last_line = line

    assert new_uuid in last_line


def test_error_handler(client):
    response = client.get('/nonexistent_endpoint')
    assert response.status_code == 404
    assert b'404, Page Not Found' in response.data
