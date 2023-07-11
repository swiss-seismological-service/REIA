FROM python:3.10-slim as builder

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl libpq-dev git\
    && rm -rf /var/lib/apt/lists/* /usr/share/doc /usr/share/man \
    && apt-get clean


COPY requirements.txt .
RUN pip wheel --no-cache-dir --no-deps --wheel-dir /app/wheels -r requirements.txt


FROM python:3.10-slim as creator

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl libpq-dev git\
    && rm -rf /var/lib/apt/lists/* /usr/share/doc /usr/share/man \
    && apt-get clean

COPY --from=builder /app/wheels /wheels
COPY --from=builder /app/requirements.txt .

RUN pip install --no-cache /wheels/*

COPY . .

RUN pip install .

RUN reia db createall


FROM postgis/postgis:latest as ria-db

ENV DB_NAME="reia" \
    DB_USER="admin" \
    DB_PASSWORD="password"

ADD ./db/postgresql.conf                                    /etc/postgresql/
ADD ./db/init_database.sh                                   /docker-entrypoint-initdb.d/00_init_database.sh
COPY --from=creator /app/create_database.sql                /docker-entrypoint-initdb.d/01_create_database.sql
ADD ./db/functions/materialized_loss_buildings.sql          /docker-entrypoint-initdb.d/02_materialized_loss_buildings.sql
ADD ./db/functions/trigger_refresh_materialized.sql         /docker-entrypoint-initdb.d/03_trigger_refresh_materialized.sql
ADD ./db/functions/trigger_partition_aggregationtags.sql    /docker-entrypoint-initdb.d/04_trigger_partition_aggregationtags.sql
ADD ./db/functions/trigger_partition_losstype.sql           /docker-entrypoint-initdb.d/05_trigger_partition_losstype.sql
ADD ./db/functions/indexes.sql                              /docker-entrypoint-initdb.d/06_indexes.sql



CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf"]