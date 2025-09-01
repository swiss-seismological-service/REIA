#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --user "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create user only if it doesn't exist
    DO \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = '$DB_USER') THEN
            CREATE USER $DB_USER with encrypted password '$DB_PASSWORD';
        END IF;
    END
    \$\$;
    
    CREATE DATABASE $DB_NAME;
    \c $DB_NAME;
    CREATE EXTENSION IF NOT EXISTS POSTGIS;
    CREATE EXTENSION IF NOT EXISTS WEIGHTED_STATISTICS;
    GRANT ALL PRIVILEGES ON DATABASE $DB_NAME TO $DB_USER;
    GRANT ALL ON SCHEMA public TO $DB_USER;
    ALTER TABLE public.spatial_ref_sys OWNER TO $DB_USER;
EOSQL