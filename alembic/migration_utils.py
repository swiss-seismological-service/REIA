"""
Utility functions for Alembic migrations in REIA project.

This module provides low-level helper functions for common migration tasks,
especially for managing custom SQL scripts like indexes, triggers,
and materialized views.

The high-level migration logic (apply/remove patterns) is implemented
directly in the revision files for better transparency and discoverability.
"""

from pathlib import Path
from typing import List, Optional

from alembic import op


def execute_sql_file(filename: str, directory: str = "functions") -> None:
    """
    Execute a SQL file from the db directory.

    Args:
        filename: Name of the SQL file to execute
        directory: Subdirectory under db/ (default: "functions")
    """
    sql_file = Path(__file__).parent.parent / "db" / directory / filename
    if sql_file.exists():
        with open(sql_file, 'r') as f:
            sql_content = f.read()
        op.execute(sql_content)
        print(f"Executed SQL file: {filename}")
    else:
        raise FileNotFoundError(f"SQL file not found: {sql_file}")


def execute_sql(sql: str, description: Optional[str] = None) -> None:
    """
    Execute raw SQL with optional description.

    Args:
        sql: SQL statement to execute
        description: Optional description for logging
    """
    op.execute(sql)
    if description:
        print(f"{description}")


def drop_indexes(index_names: List[str]) -> None:
    """
    Drop multiple indexes safely.

    Args:
        index_names: List of index names to drop
    """
    for index_name in index_names:
        op.execute(f"DROP INDEX IF EXISTS {index_name};")
    print(f"Dropped {len(index_names)} indexes")


def drop_triggers(trigger_specs: List[tuple]) -> None:
    """
    Drop multiple triggers safely.

    Args:
        trigger_specs: List of (trigger_name, table_name) tuples
    """
    for trigger_name, table_name in trigger_specs:
        op.execute(f"DROP TRIGGER IF EXISTS {trigger_name} ON {table_name};")
    print(f"Dropped {len(trigger_specs)} triggers")


def drop_functions(function_names: List[str]) -> None:
    """
    Drop multiple functions safely.

    Args:
        function_names: List of function names to drop
    """
    for function_name in function_names:
        op.execute(f"DROP FUNCTION IF EXISTS {function_name}();")
    print(f"Dropped {len(function_names)} functions")


def drop_materialized_views(view_names: List[str]) -> None:
    """
    Drop multiple materialized views safely.

    Args:
        view_names: List of materialized view names to drop
    """
    for view_name in view_names:
        op.execute(f"DROP MATERIALIZED VIEW IF EXISTS {view_name};")
    print(f"Dropped {len(view_names)} materialized views")


def create_extension(extension_name: str) -> None:
    """
    Create PostgreSQL extension if not exists.

    Args:
        extension_name: Name of the extension to create
    """
    op.execute(f"CREATE EXTENSION IF NOT EXISTS {extension_name};")
    print(f"Created extension: {extension_name}")
