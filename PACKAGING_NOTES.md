# REIA Package Distribution Notes

This document explains how REIA is configured for distribution via PyPI and how the Alembic migrations work in both development and installed package modes.

## Package Structure

When REIA is installed via `pip install reia`, the following files are included:

### Python Code
- `reia/` - Main Python package
- `settings/` - Configuration modules

### Alembic Migration System
- `alembic.ini` - Alembic configuration
- `alembic/env.py` - Alembic environment setup
- `alembic/script.py.mako` - Migration template
- `alembic/migration_utils.py` - Utility functions for migrations
- `alembic/versions/*.py` - Migration files
- `alembic/README` - Alembic documentation

### Database Scripts
- `db/functions/*.sql` - Custom SQL scripts (indexes, triggers, materialized views)
- `db/init_database.sh` - PostgreSQL initialization script
- `db/pg_hba.conf` - PostgreSQL authentication configuration
- `db/postgresql.conf` - PostgreSQL server configuration

### Documentation
- `README.md` - Project documentation
- `MIGRATION_GUIDE.md` - Database migration guide
- `.env.example` - Environment configuration template

## How Alembic Works in Installed Packages

### Path Resolution
The CLI commands automatically detect whether REIA is running in:
1. **Development mode**: Uses `alembic.ini` in current directory
2. **Installed mode**: Uses `alembic.ini` from the installed package location

### Migration Utilities
Each migration file includes logic to import `migration_utils.py` from:
1. **Development**: Direct import from `alembic/migration_utils.py`
2. **Installed**: Dynamic import using `importlib.util`

### SQL Script Access
Migration utilities find SQL scripts in `db/functions/` relative to the package installation directory.

## Configuration Files

### pyproject.toml
```toml
[tool.setuptools]
include-package-data = true

[tool.setuptools.package-data]
"*" = ["*.xml", "*.ini"]
```

### MANIFEST.in
Specifies which non-Python files to include in the distribution:
- Alembic configuration and migrations
- Database scripts and configuration
- Documentation files
- Environment templates

## Usage After Installation

### From PyPI
```bash
pip install reia
reia db migrate
```

### From Source
```bash
git clone <repo>
cd REIA
pip install -e .[dev]
reia db migrate
```

Both methods work identically - the CLI automatically detects the mode and finds the correct file paths.

## Testing Package Distribution

### Build Source Distribution
```bash
python -m build --sdist
```

### Check Contents
```bash
tar -tzf dist/reia-*.tar.gz | grep -E "(alembic|db)"
```

### Install and Test
```bash
pip install dist/reia-*.tar.gz
reia db current  # Should work without errors
```

## Migration Development

### Creating New Migrations
```bash
# Auto-generate from model changes
alembic revision --autogenerate -m "Add new feature"

# Manual migration
alembic revision -m "Custom database changes"
```

### Using Migration Utilities
New migrations automatically have access to utility functions:
```python
def upgrade() -> None:
    # Use any of these functions
    execute_sql_file("new_indexes.sql")
    create_extension("hstore")
    apply_reia_sql_scripts()  # Apply all standard scripts
```

## File Inclusion Rules

### Included in Package
- All Python files
- `alembic.ini` and `alembic/` directory
- `db/` directory with SQL scripts
- Documentation files
- `.env.example`

### Excluded from Package
- `.env` (sensitive data)
- `docker-compose.yml` (development only)
- `setup.sh` (development script)
- `logs/`, `env/` (runtime/development directories)
- Git and cache files

## Best Practices

1. **Test both modes**: Always test migrations in development and after package installation
2. **Keep SQL scripts**: Store custom SQL in `db/functions/` for proper packaging
3. **Use migration utilities**: Prefer utility functions over raw SQL for consistency
4. **Version migrations**: Each schema change should have its own migration file
5. **Document changes**: Include clear descriptions in migration messages

## Troubleshooting

### Common Issues
- **ModuleNotFoundError**: Check that `MANIFEST.in` includes all required files
- **FileNotFoundError**: Verify SQL scripts are in `db/functions/` directory
- **Path errors**: Ensure CLI path detection logic works for your use case

### Debugging
```bash
# Check package contents
python -c "import reia; print(reia.__file__)"

# Verify Alembic files
reia db current  # Should find alembic.ini automatically

# Test migration utilities
python -c "from alembic.migration_utils import execute_sql_file; print('OK')"
```