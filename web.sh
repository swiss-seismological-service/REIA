#!/bin/bash

source env/bin/activate

# use flask cli, custom cli commands are not available
flask run

# use own cli, need to install first with 'pip install -e .'
# ebr run

deactivate
