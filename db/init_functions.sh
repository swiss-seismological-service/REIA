#!/bin/bash
set -e

psql -d $DB_NAME -U $DB_USER -f /etc/postgresql/create_database.sql \
                             -f /etc/postgresql/materialized_loss_buildings.sql \
                             -f /etc/postgresql/trigger_refresh_materialized.sql \
                             -f /etc/postgresql/trigger_partition_aggregationtags.sql \
                             -f /etc/postgresql/trigger_partition_losstype.sql \
                             -f /etc/postgresql/indexes.sql