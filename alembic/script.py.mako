"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
import sys
from pathlib import Path
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
${imports if imports else ""}

# revision identifiers, used by Alembic.
revision: str = ${repr(up_revision)}
down_revision: Union[str, Sequence[str], None] = ${repr(down_revision)}
branch_labels: Union[str, Sequence[str], None] = ${repr(branch_labels)}
depends_on: Union[str, Sequence[str], None] = ${repr(depends_on)}


def _get_migration_utils():
    """Import migration utilities, handling both development and installed package modes."""
    # Add migration_utils to path
    sys.path.append(str(Path(__file__).parent.parent))
    
    try:
        from migration_utils import (
            execute_sql_file, execute_sql, apply_reia_sql_scripts, 
            remove_reia_sql_scripts, drop_indexes, drop_triggers, 
            drop_functions, drop_materialized_views, create_extension
        )
        return {
            'execute_sql_file': execute_sql_file,
            'execute_sql': execute_sql,
            'apply_reia_sql_scripts': apply_reia_sql_scripts,
            'remove_reia_sql_scripts': remove_reia_sql_scripts,
            'drop_indexes': drop_indexes,
            'drop_triggers': drop_triggers,
            'drop_functions': drop_functions,
            'drop_materialized_views': drop_materialized_views,
            'create_extension': create_extension,
        }
    except ImportError:
        # Fallback for when running from installed package
        import importlib.util
        utils_path = Path(__file__).parent.parent / "migration_utils.py"
        spec = importlib.util.spec_from_file_location("migration_utils", utils_path)
        migration_utils = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(migration_utils)
        return {
            'execute_sql_file': migration_utils.execute_sql_file,
            'execute_sql': migration_utils.execute_sql,
            'apply_reia_sql_scripts': migration_utils.apply_reia_sql_scripts,
            'remove_reia_sql_scripts': migration_utils.remove_reia_sql_scripts,
            'drop_indexes': migration_utils.drop_indexes,
            'drop_triggers': migration_utils.drop_triggers,
            'drop_functions': migration_utils.drop_functions,
            'drop_materialized_views': migration_utils.drop_materialized_views,
            'create_extension': migration_utils.create_extension,
        }


# Load migration utilities
_utils = _get_migration_utils()
execute_sql_file = _utils['execute_sql_file']
execute_sql = _utils['execute_sql']
apply_reia_sql_scripts = _utils['apply_reia_sql_scripts']
remove_reia_sql_scripts = _utils['remove_reia_sql_scripts']
drop_indexes = _utils['drop_indexes']
drop_triggers = _utils['drop_triggers']
drop_functions = _utils['drop_functions']
drop_materialized_views = _utils['drop_materialized_views']
create_extension = _utils['create_extension']


def upgrade() -> None:
    """Upgrade schema."""
    ${upgrades if upgrades else "pass"}


def downgrade() -> None:
    """Downgrade schema."""
    ${downgrades if downgrades else "pass"}
