
import os
from io import StringIO
from multiprocessing import Pool

import numpy as np
import pandas as pd
import psycopg2


def copy_pooled(df, tablename, max_procs, max_entries=750000):
    nprocs = 1
    while len(df) / nprocs > max_entries and nprocs < max_procs:
        nprocs += 1

    chunks = df.groupby(
        np.arange(len(df)) // (len(df) / nprocs))

    pool_args = [(chunk, tablename)
                 for _, chunk in chunks]

    with Pool(nprocs) as pool:
        pool.starmap(copy_raw, pool_args)


def copy_raw(df, tablename):
    connect_text = \
        f"dbname='{os.getenv('DB_NAME')}' " \
        f"user='{os.getenv('DB_USER')}' " \
        f"host={os.getenv('POSTGRES_HOST')} " \
        f"port={os.getenv('POSTGRES_PORT')} " \
        f"password='{os.getenv('DB_PASSWORD')}'"

    conn = psycopg2.connect(connect_text)
    cursor = conn.cursor()
    copy_from_dataframe(cursor, df, tablename)
    conn.commit()
    conn.close()


def copy_from_dataframe(cursor, df: pd.DataFrame, table: str):
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, header=False, index=False)
    buffer.seek(0)

    try:
        cursor.copy_from(buffer, table, sep=",", columns=df.columns)
    except (Exception, psycopg2.DatabaseError) as err:
        cursor.close()
        raise err


def get_nextval(cursor, table: str, column: str):
    # set sequence to correct number
    cursor.execute(
        f"SELECT setval(pg_get_serial_sequence('{table}', '{column}'), "
        f"coalesce(max({column}),0) + 1, false) FROM {table};"
    )
    # get nextval
    cursor.execute(
        f"select nextval(pg_get_serial_sequence('{table}', '{column}'))")
    next = cursor.fetchone()[0]
    return next
