#!/bin/bash
set -e

docker build -t reia_db .
docker run --env-file=.env -e POSTGRES_PASSWORD=postgres -e POSTGRES_USER=postgres -p 5432:5432 reia_db

source env/bin/activate
reia exposure add reia/db/tests/data/exposure_mim_rb.xml test
reia vulnerability add reia/db/tests/data/struc_vul.xml test

python test_calculation.py
deactivate