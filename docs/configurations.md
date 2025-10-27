# REIA Application Configuration

For local use, the default values in `.env.example` can be copied to `.env` and left unchanged. For production deployments, the environment variables should be set according to the deployment environment.  Following, the configuration options are described:

#### `MAX_PROCESSES`  
Maximum number of processes to use for parallel tasks like copying large amounts of rows to the database.

#### `LOG_LEVEL`  
Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL (default: INFO)

#### `AGENCY_ID`  
Agency ID, will be associated with new risk assessments. (creationinfo_agencyid)

#### `DB_NAME`, `DB_USER`, `DB_PASSWORD`  
Database connection credentials. RW user for the CLI and webservice.

#### `POSTGRES_HOST`, `POSTGRES_PORT`  
Host and port of the PostgreSQL database.

#### `POSTGRES_USER`, `POSTGRES_PASSWORD`  
Superuser credentials. Used by the CLI for database initialization.

#### `POSTGRES_POOL_SIZE`, `POSTGRES_MAX_OVERFLOW`  
Connection pool settings for the ORM layer database connections.

#### `POSTGRES_PGCONF`, `POSTGRES_PGHBA`, `POSTGRES_DATADIR`
Paths to the docker volume mounts for PostgreSQL configuration files and data directory. Defaults will be used if not set.

#### `ROOT_PATH`  
Root path of the REIA webservice. I.e. the path segment after the host URL and before the webservice version (eg. `/reiaws`).

#### `OQ_HOST`, `OQ_USER`, `OQ_PASSWORD`, `OQ_ADMIN_EMAIL`, `OQ_PORT`, `OQ_VERSION`  
Credentials and connection settings for the OpenQuake container. OQ Version should currently be set to 14 for scenario calculations (which are done with OQ 3.14) and >14 otherwise. The criteria is only <= or > 14.

#### `ALLOW_ORIGIN_REGEX`  
CORS settings, allows frontend to access webservice, can be set eg. to `https?:\/\/(.*)\.ethz\.ch(.*)` to allow all subdomains of ethz.ch.

#### `ALLOW_ORIGINS`  
Same as above but as a list. Can be set in addition to the regex, eg. `["http://localhost","http://localhost:5000"]`

#### `CSV_NAMES_PATH`  
Defaults is `app/config/csv_settings.json`, allows to customize the column names in the output CSV files. Custom mount or build for the webservice container is needed however if the file or path are changed.

#### `WEB_THREADS`  
Number of threads spawned by `gunicorn` to run the webservice.

#### `WEB_CONCURRENCY`  
Number of workers spawned by `gunicorn` per thread to run the webservice.

#### `WEB_RELOAD`  
If set to `true`, `gunicorn` will reload the webservice upon code changes. Only useful in development environments, usually set to `false`.