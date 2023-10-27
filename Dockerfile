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


FROM postgis/postgis:16-3.4 as reia-db

ENV DB_NAME="reia" \
    DB_USER="admin" \
    DB_PASSWORD="password"

ADD ./db/pg_hba.conf                                        /etc/postgresql/
ADD ./db/postgresql.conf                                    /etc/postgresql/
ADD ./db/init_database.sh                                   /docker-entrypoint-initdb.d/00_init_database.sh
ADD ./db/init_functions.sh                                  /docker-entrypoint-initdb.d/01_init_functions.sh
COPY --from=creator /app/create_database.sql                /etc/postgresql/
ADD ./db/functions/materialized_loss_buildings.sql          /etc/postgresql/
ADD ./db/functions/trigger_refresh_materialized.sql         /etc/postgresql/
ADD ./db/functions/trigger_partition_aggregationtags.sql    /etc/postgresql/
ADD ./db/functions/trigger_partition_losstype.sql           /etc/postgresql/
ADD ./db/functions/indexes.sql                              /etc/postgresql/

CMD ["postgres", "-c", "config_file=/etc/postgresql/postgresql.conf", "-c", "hba_file=/etc/postgresql/pg_hba.conf"]