from contextlib import contextmanager
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


def copy_pooled(df, tablename, max_entries=750_000):

    max_procs = int(os.getenv('MAX_PROCESSES', '2'))

    nprocs = max(1, min(max_procs, int(np.ceil(len(df) / max_entries))))
    nprocs = 2
    chunks = np.array_split(df, nprocs)

    pool_args = [(chunk, tablename) for chunk in chunks]

    with Pool(nprocs) as pool:
        pool.starmap(copy_raw, pool_args)


def copy_raw(df, tablename):
    conn = make_connection()
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SET synchronous_commit TO OFF;")
            logging.info(f"Copying {len(df)} rows to {tablename}...")
            copy_from_dataframe(cursor, df, tablename)
    finally:
        conn.close()


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
            logging.info(f"Successfully copied {len(df)} rows to {table}.")
    except Exception as err:
        logging.error(f"Error copying to {table}: {err}")
        raise


def allocate_oids(cursor, table: str, column: str, count: int) -> list[int]:
    cursor.execute(
        """
        SELECT nextval(pg_get_serial_sequence(%s, %s))
        FROM generate_series(1, %s)
        """,
        (table, column, count)
    )
    return [row[0] for row in cursor.fetchall()]


@contextmanager
def db_cursor_from_session(session):
    connection = session.get_bind().raw_connection()
    try:
        with connection.cursor() as cursor:
            yield cursor
        connection.commit()
    except Exception:
        connection.rollback()
        raise
    finally:
        connection.close()
