import pandas as pd
import pytest
from psycopg2.extensions import connection

from reia.repositories.utils import (copy_from_dataframe, copy_pooled,
                                     copy_raw, db_cursor_from_session,
                                     drop_dynamic_table, make_connection)


@pytest.fixture(scope='function')
def test_table(db_session):
    """
    Fixture to create a test table in the database.
    """
    with db_cursor_from_session(db_session) as cursor:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS test_table (
                id SERIAL PRIMARY KEY,
                name VARCHAR(50),
                value FLOAT
            );
        """)
    yield 'test_table'
    with db_cursor_from_session(db_session) as cursor:
        cursor.execute("DROP TABLE IF EXISTS test_table;")
        db_session.commit()


def test_copy_from_dataframe(db_session, test_table):

    # create a sample DataFrame
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C'],
        'value': [10.5, 20.0, 30.5]
    })

    with db_cursor_from_session(db_session) as cursor:
        # use the copy_from_dataframe function to copy the DataFrame to the
        # database
        copy_from_dataframe(cursor, df, 'test_table')

    with db_cursor_from_session(db_session) as cursor:
        # verify that the data was copied correctly
        cursor.execute("SELECT * FROM test_table ORDER BY id;")
        rows = cursor.fetchall()
        assert rows == [(1, 'A', 10.5), (2, 'B', 20.0), (3, 'C', 30.5)]


def test_copy_pooled(db_session, test_table):
    """
    Test the copy_from_dataframe function with a pooled connection.
    """
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C'],
        'value': [10.5, 20.0, 30.5]
    })

    copy_pooled(df, 'test_table', max_entries=2)

    with db_cursor_from_session(db_session) as cursor:
        cursor.execute("SELECT * FROM test_table ORDER BY id;")
        rows = cursor.fetchall()
        assert rows == [(1, 'A', 10.5), (2, 'B', 20.0), (3, 'C', 30.5)]


def test_make_connection():
    conn = make_connection()
    assert isinstance(conn, connection)


def test_copy_raw(db_session, test_table):
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['A', 'B', 'C'],
        'value': [10.5, 20.0, 30.5]
    })

    copy_raw(df, test_table)

    with db_cursor_from_session(db_session) as cursor:
        cursor.execute("SELECT * FROM test_table ORDER BY id;")
        rows = cursor.fetchall()
        assert rows == [(1, 'A', 10.5), (2, 'B', 20.0), (3, 'C', 30.5)]

    drop_dynamic_table(db_session.get_bind(), test_table)

    with db_cursor_from_session(db_session) as cursor:
        # check if table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables
                WHERE table_name = %s
            );
        """, (test_table,))
        exists = cursor.fetchone()[0]
        assert not exists, "Table should be dropped after test"
