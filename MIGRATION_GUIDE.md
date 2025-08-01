# REIA Database Migration Guide

This guide explains how to use Alembic database migrations in the REIA project.

## Overview

REIA now uses Alembic for database schema management, providing:
- **Version control** for database changes
- **Easy deployment** on both Docker and system PostgreSQL
- **Rollback capability** for schema changes
- **Team collaboration** with trackable database evolution

## Quick Start

### Setting up a new database

1. **Ensure environment is activated**:
   ```bash
   source env/bin/activate
   ```

2. **Configure database connection** in `.env` file:
   ```bash
   DB_NAME=your_database
   DB_USER=your_user
   DB_PASSWORD=your_password
   POSTGRES_HOST=localhost
   POSTGRES_PORT=5432
   ```

3. **Run migrations**:
   ```bash
   reia db migrate
   ```

This will create all tables, indexes, triggers, and materialized views.

### Working with existing database

If you have an existing REIA database, you need to stamp it with the current revision:

```bash
# Stamp the database to mark it as up-to-date
reia db stamp head

# Check current revision
reia db current
```

## Available Commands

### Migration Commands

| Command | Description |
|---------|-------------|
| `reia db migrate` | Run all pending migrations |
| `reia db current` | Show current database revision |
| `reia db history` | Show migration history |
| `reia db downgrade -1` | Downgrade to previous revision |
| `reia db downgrade <revision>` | Downgrade to specific revision |
| `reia db stamp head` | Mark database as current without running migrations |

### Legacy Commands (for backward compatibility)

| Command | Description |
|---------|-------------|
| `reia db init` | Initialize database (old method) |
| `reia db drop` | Drop all tables |
| `reia db createall` | Generate SQL file (for Docker) |

## Migration Workflow

### For Developers

When you make changes to SQLAlchemy models:

1. **Generate a new migration**:
   ```bash
   alembic revision --autogenerate -m "Description of changes"
   ```

2. **Review the generated migration** in `alembic/versions/`

3. **Apply the migration**:
   ```bash
   reia db migrate
   ```

### For Custom SQL Scripts

When adding new SQL scripts (indexes, triggers, etc.):

1. **Place SQL file** in `db/functions/`

2. **Create a migration**:
   ```bash
   alembic revision -m "Add new indexes"
   ```

3. **Edit the migration** to use utility functions:
   ```python
   from migration_utils import execute_sql_file
   
   def upgrade():
       execute_sql_file("your_new_script.sql")
   
   def downgrade():
       # Add cleanup SQL
       op.execute("DROP INDEX IF EXISTS your_index;")
   ```

## File Structure

```
├── alembic/
│   ├── versions/           # Migration files
│   ├── env.py             # Alembic environment config
│   ├── migration_utils.py # Utility functions
│   └── script.py.mako     # Migration template
├── alembic.ini            # Alembic configuration
└── db/
    └── functions/         # Custom SQL scripts
```

## Utility Functions

The `alembic/migration_utils.py` module provides helper functions:

- `execute_sql_file(filename)` - Execute SQL file from db/functions/
- `apply_reia_sql_scripts()` - Apply all REIA custom scripts
- `remove_reia_sql_scripts()` - Remove all REIA custom scripts
- `drop_indexes(names)` - Drop multiple indexes safely
- `drop_triggers(specs)` - Drop multiple triggers safely

## Docker Integration

REIA now uses Docker Compose for easy service management:

### Docker Compose Setup (Recommended)

1. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your settings
   ```

2. **Start services**:
   ```bash
   docker-compose up -d
   ```

3. **Apply migrations**:
   ```bash
   reia db migrate
   ```

4. **Check services**:
   ```bash
   docker-compose ps
   ```

### Available Services

The docker-compose.yml provides:

- **PostgreSQL** (port 5432): Database with PostGIS extension
- **OpenQuake** (port 8800): Seismic hazard and risk calculations

### Service Management

```bash
# Start services
docker-compose up -d

# View logs
docker-compose logs -f postgres
docker-compose logs -f openquake

# Stop services
docker-compose down

# Complete cleanup (removes volumes)
docker-compose down -v

# Restart specific service
docker-compose restart postgres
```

### Environment Variables

The docker-compose.yml uses environment variables from `.env`:

- `DB_NAME`, `DB_USER`, `DB_PASSWORD` - Database credentials
- `POSTGRES_HOST`, `POSTGRES_PORT` - Database connection
- `OQ_USER`, `OQ_PASSWORD`, `OQ_ADMIN_EMAIL` - OpenQuake credentials

## Troubleshooting

### Common Issues

**"No revision" error**: Database needs to be stamped
```bash
reia db stamp head
```

**Migration conflicts**: Check for uncommitted changes
```bash
reia db history
alembic show head
```

**Custom SQL errors**: Check that SQL files exist in `db/functions/`

### Reset Database

To completely reset the database:
```bash
reia db drop
reia db migrate
```

### Rollback Changes

To undo the last migration:
```bash
reia db downgrade -1
```

To rollback to a specific revision:
```bash
reia db downgrade <revision_id>
```

## Best Practices

1. **Always backup** production databases before migrations
2. **Test migrations** on a copy of production data
3. **Review generated migrations** before applying
4. **Use descriptive names** for migration messages
5. **Keep custom SQL scripts** in version control
6. **Document breaking changes** in migration comments

## Migration History

Current migrations:
- `bb9bd27f1af5` - Initial database schema and custom functions

## Support

For issues with database migrations:
1. Check the migration logs
2. Verify database connection settings
3. Ensure all required SQL files are present
4. Consult the Alembic documentation: https://alembic.sqlalchemy.org/