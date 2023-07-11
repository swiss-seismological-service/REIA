#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --user "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER $DB_USER with encrypted password '$DB_PASSWORD';
    CREATE DATABASE $DB_NAME;
    \c $DB_NAME;
    CREATE EXTENSION IF NOT EXISTS POSTGIS;
    GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;
    GRANT ALL ON SCHEMA public TO $DB_USER;
    ALTER TABLE public.spatial_ref_sys OWNER TO $DB_USER;
EOSQL

psql -d $DB_NAME -U $DB_USER -f /etc/postgresql/municipalities.sql