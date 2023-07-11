#!/bin/bash
set -e

set -a
. .env
set +a

psql -d $DB_NAME -U $DB_USER --host $POSTGRES_HOST  -f $(pwd)/db/functions/materialized_loss_buildings.sql \
                                                    -f $(pwd)/db/functions/trigger_refresh_materialized.sql \
                                                    -f $(pwd)/db/functions/trigger_partition_aggregationtags.sql \
                                                    -f $(pwd)/db/functions/trigger_partition_losstype.sql \
                                                    -f $(pwd)/db/functions/indexes.sql \
