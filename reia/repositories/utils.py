from contextlib import contextmanager
from io import StringIO
from multiprocessing import Pool

import numpy as np
import pandas as pd
import psycopg2
from psycopg2 import sql
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from sqlalchemy.engine import Connection
from sqlalchemy.sql import text

from reia.config.settings import get_settings
from reia.services.logger import LoggerService

logger = LoggerService.get_logger(__name__)


def make_connection():
    config = get_settings()
    return psycopg2.connect(
        dbname=config.db_name,
        user=config.db_user,
        host=config.postgres_host,
        port=config.postgres_port,
        password=config.db_password,
    )


def copy_pooled(df, tablename, max_entries=750_000):
    config = get_settings()
    max_procs = config.max_processes

    nprocs = max(1, min(max_procs, int(np.ceil(len(df) / max_entries))))

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
            logger.info(f"Copying {len(df)} rows to {tablename}...")
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
            logger.info(f"Successfully copied {len(df)} rows to {table}.")
    except Exception as err:
        logger.error(f"Error copying to {table}: {err}")
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


def drop_dynamic_table(connection: Connection, table_name: str):
    """
    Truncates and drops a standalone dynamically named table.
    Example: 'loss_assoc_<int>'
    """
    with connection.connect() as conn:
        conn.execute(text(f"""
            TRUNCATE TABLE {table_name};
            DROP TABLE {table_name};
        """))
        conn.commit()


def drop_partition_table(connection: Connection,
                         parent_table: str,
                         partition_suffix: int):
    """
    Detaches, truncates, and drops a child partition from a parent table.
    Example: 'loss_riskvalue_<int>' from 'loss_riskvalue'.
    """
    partition_table = f"{parent_table}_{partition_suffix}"
    with connection.connect() as conn:
        conn.execute(text(f"""
            ALTER TABLE {parent_table} DETACH PARTITION {partition_table};
            TRUNCATE TABLE {partition_table};
            DROP TABLE {partition_table};
        """))
        conn.commit()
