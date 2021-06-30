#!/bin/bash

source env/bin/activate

# use flask cli, custom cli commands are not available
celery -A 'wsgi:celery' worker --pool=solo --loglevel=info &
flask run

deactivate
