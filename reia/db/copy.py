import logging
import os
from io import StringIO
from multiprocessing import Pool

import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

logging.basicConfig(level=logging.INFO)


def make_connection():
    return psycopg2.connect(
        dbname=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        host=os.getenv('POSTGRES_HOST'),
        port=os.getenv('POSTGRES_PORT'),
        password=os.getenv('DB_PASSWORD'),
    )


def copy_pooled(df, tablename, max_procs, max_entries=750_000):
    nprocs = 1
    while len(df) / nprocs > max_entries and nprocs < max_procs:
        nprocs += 1

    chunks = df.groupby(np.arange(len(df)) // (len(df) / nprocs))
    pool_args = [(chunk, tablename) for _, chunk in chunks]

    with Pool(nprocs) as pool:
        pool.starmap(copy_raw, pool_args)


def copy_raw(df, tablename):
    conn = make_connection()
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    with conn:
        with conn.cursor() as cursor:
            cursor.execute("SET synchronous_commit TO OFF;")
            logging.info(f"Copying {len(df)} rows to {tablename}...")
            copy_from_dataframe(cursor, df, tablename)


def copy_from_dataframe(cursor, df: pd.DataFrame, table: str):
    try:
        with StringIO() as buffer:
            df.to_csv(buffer, header=False, index=False, encoding='utf-8')
            buffer.seek(0)
            columns = sql.SQL(', ').join(map(sql.Identifier, df.columns))
            stmt = sql.SQL("COPY {} ({}) FROM STDIN WITH CSV").format(
                sql.Identifier(table), columns
            )
            cursor.copy_expert(stmt, buffer)
    except (Exception, psycopg2.DatabaseError) as err:
        logging.error(f"Error copying to {table}: {err}")
        raise


def get_nextval(cursor, table: str, column: str):
    cursor.execute(
        f"SELECT setval(pg_get_serial_sequence('{table}', '{column}'), "
        f"coalesce(max({column}),0) + 1, false) FROM {table};"
    )
    cursor.execute(
        f"SELECT nextval(pg_get_serial_sequence('{table}', '{column}'))"
    )
    return cursor.fetchone()[0]


def reset_sequence(cursor, table: str, column: str):
    cursor.execute(
        f"SELECT setval(\n"
        f"  pg_get_serial_sequence('{table}', '{column}'),\n"
        f"  (SELECT MAX({column}) FROM {table}) + 1\n"
        f");"
    )
