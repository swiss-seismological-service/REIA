#!/bin/bash

source env/bin/activate

# use flask cli, custom cli commands are not available
celery -A wsgi:celery_app worker --pool=solo --loglevel=info &
flask run

# use own cli, need to install first with 'pip install -e .'
# ebr run

deactivate
