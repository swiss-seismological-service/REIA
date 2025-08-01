import os
import sys
from logging.config import fileConfig

from sqlalchemy import engine_from_config, pool  # noqa

from alembic import context
from reia.datamodel import (asset, calculations, exposure, fragility,  # noqa
                            lossvalues, vulnerability)
from reia.datamodel.base import ORMBase
from settings import get_config

# Add the project root to sys.path so we can import our models
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

# Import REIA models and configuration

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Import all models to ensure they're registered with SQLAlchemy

# Set target_metadata to our REIA models
target_metadata = ORMBase.metadata

# Get REIA configuration
reia_config = get_config()

# other values from the config, defined by the needs of env.py,
# can be acquired:
# my_important_option = config.get_main_option("my_important_option")
# ... etc.


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.

    """
    url = reia_config.DB_CONNECTION_STRING
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    In this scenario we need to create an Engine
    and associate a connection with the context.

    """
    # Use REIA's database configuration
    from reia.repositories import engine

    with engine.connect() as connection:
        context.configure(
            connection=connection,
            target_metadata=target_metadata,
            compare_type=True,
            compare_server_default=True,
        )

        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
