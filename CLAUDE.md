# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Environment Setup
```bash
# Activate virtual environment first
source env/bin/activate

# Install in development mode
pip install -e .[dev]
```

### Testing
```bash
pytest --cov=.
```

### Code Quality
```bash
# Run linting
flake8

# Auto-format code
autopep8 --in-place --recursive reia/

# Sort imports
isort reia/
```

### Database Operations

#### Alembic Migrations (Recommended)
```bash
# Run all migrations to set up database
reia db migrate

# Show current database revision
reia db current

# Show migration history
reia db history

# Downgrade to previous revision
reia db downgrade -1

# Downgrade to specific revision
reia db downgrade <revision_id>

# Stamp database with revision (without running migrations)
reia db stamp head
```

#### Legacy Database Operations
```bash
# Initialize database tables (old method)
reia db init

# Drop all tables
reia db drop

# Create all tables to file (for Docker builds)
reia db createall
```

### Docker Operations
```bash
# Start all services (PostgreSQL + OpenQuake)
docker-compose up -d

# Check service status
docker-compose ps

# View logs
docker-compose logs -f

# Set up database schema (after services are running)
reia db migrate

# Stop services
docker-compose down

# Stop services and remove volumes (complete cleanup)
docker-compose down -v
```

## Development Notes
- **Virtual Environment**: Always activate the virtual environment before running Python commands: `source env/bin/activate`
- **Python Version**: Project requires Python ≤3.12 (currently using Python 3.10.12)
- **Installation**: Use `pip install -e .[dev]` for development installation with test dependencies
- **Package Structure**: Modern Python packaging using `pyproject.toml` instead of `setup.py`
- **Database Migrations**: Uses Alembic for schema versioning. Run `reia db migrate` to set up fresh databases.
- **Custom SQL Scripts**: Stored in `db/functions/` and managed through Alembic migrations for consistency

## Architecture Overview

REIA is a Rapid Earthquake Impact Assessment tool that integrates with OpenQuake engine for seismic risk calculations.

**New Schema Layer**: The project now includes a comprehensive schema layer (`reia/schemas/`) with Pydantic models for data validation:
- `asset_schemas.py`: Asset and exposure validation schemas
- `calculation_schemas.py`: Calculation and risk assessment schemas
- `fragility_schemas.py`, `vulnerability_schemas.py`: Model validation schemas
- `lossvalue_schemas.py`: Loss and risk value schemas
- `enums.py`: Shared enumerations and constants
- `base.py`: Base schema patterns

### Core Components

**CLI Interface (`reia/cli.py`)**
- Main entry point using Typer framework
- Commands organized by domain: db, exposure, vulnerability, fragility, taxonomymap, calculation, risk-assessment
- Thin interface layer that delegates business logic to service classes
- Each domain provides CRUD operations (add, delete, list, create_file)

**Service Layer (`reia/services/`)**
- **`RiskAssessmentService`**: Orchestrates complete risk assessment workflows
- **`CalculationService`**: Manages OpenQuake calculations with proper error handling
- **`ResultsService`**: Handles OpenQuake result processing and database storage
- **`StatusTracker`**: Centralized status management with validation and consistent logging
- **`OQCalculationAPI`**: OpenQuake engine API client for calculation management
- **`LoggerService`**: Centralized logging configuration and management
- **Domain-specific services**: `ExposureService`, `VulnerabilityService`, `FragilityService`, `TaxonomyService`

**Data Models (`reia/datamodel/`)**
- SQLAlchemy ORM models for earthquake risk assessment
- `base.py`: Base ORM configuration with PostgreSQL+PostGIS
- `asset.py`: Exposure models, assets, sites, aggregation data
- `exposure.py`: Exposure-specific models and relationships
- `vulnerability.py`: Vulnerability functions and models by building type
- `fragility.py`: Fragility functions for damage states
- `calculations.py`: OpenQuake calculation tracking and risk assessments
- `lossvalues.py`: Loss, damage, and risk value storage
- `mixins.py`: Common model mixins and utilities

**Repository Layer (`reia/repositories/`)**
- Repository pattern for database operations
- `calculation.py`: CRUD operations for calculations and risk assessments
- `asset.py`: Asset and exposure model operations
- `vulnerability.py`, `fragility.py`: Model-specific operations
- `lossvalue.py`: Loss and risk value operations
- `base.py`: Base repository patterns and utilities
- `types.py`: Type definitions for repository layer
- `utils.py`: Common repository utilities
- PostgreSQL database with PostGIS extension for spatial data
- Materialized views for performance optimization

**OpenQuake Integration (`reia/services/oq_api.py`)**
- **`OQCalculationAPI`**: Complete synchronous API client for OpenQuake engine
- **`APIConnection`**: Base connection handling for OpenQuake API
- Calculation submission, monitoring, and result retrieval
- No threading - simple synchronous operations
- Remote calculation import functionality
- Proper error handling and status management

**I/O Operations (`reia/io/`)**
- `read.py`: Parse XML exposure, vulnerability, fragility models
- `write.py`: Generate OpenQuake input files from database
- `calculation.py`: Calculation input validation and parsing

### Data Flow

1. **Data Import**: Parse XML models → Store in PostgreSQL via Repository layer
2. **Calculation Setup**: Generate OpenQuake input files from database via I/O layer
3. **Calculation Execution**: Submit to OpenQuake engine via `OQCalculationAPI`
4. **Status Management**: Track calculation progress via `StatusTracker` with validation
5. **Result Processing**: Import calculation results back to database via `ResultsService`
6. **Risk Assessment**: Orchestrate complete workflows via `RiskAssessmentService`

### Configuration

- Project configuration in `pyproject.toml` (modern Python packaging)
- Environment variables loaded from `.env` file
- Database connection via `settings/config.py`
- OpenQuake API credentials in environment variables
- Logging configuration in `settings/logger.ini`
- Dependencies managed via `pyproject.toml` and `requirements.txt`

### Testing Structure

- Test data in `reia/tests/data/` with sample geometries, exposure models
- Test configuration in `conftest.py` for pytest setup
- Separate test suites: `test_api.py`, `test_geometries.py`, `test_ria.py`, `test_logging.py`
- Uses pytest with coverage reporting
- Test environment configuration via `pytest-env`

### Key Dependencies

- **OpenQuake Engine**: Core seismic hazard and risk calculations (v3.16)
- **SQLAlchemy + GeoAlchemy2**: ORM with spatial data support
- **PostGIS**: Spatial database operations
- **GeoPandas**: Spatial data manipulation
- **Typer**: CLI framework
- **Requests**: OpenQuake API communication
- **Pydantic**: Data validation and settings management
- **Jinja2**: Template rendering for OpenQuake input files
- **Psycopg2**: PostgreSQL database adapter

### Modern Service-Oriented Architecture

The system follows a clean service-oriented architecture with clear separation of concerns:

**CLI Layer** → **Service Layer** → **Repository Layer** → **Database**

**Key Architecture Principles:**
- **Service Layer Pattern**: Business logic centralized in dedicated service classes
- **Repository Pattern**: Database operations abstracted behind repository interfaces  
- **Status Management**: Centralized status tracking with validation and audit logging
- **Synchronous Operations**: Simple, straightforward execution flow without threading complexity
- **Clear Separation**: Each layer has distinct responsibilities and minimal coupling

**Benefits:**
- **Testability**: Service classes are easily unit tested and mockable
- **Maintainability**: Business logic is isolated and clearly organized
- **Reliability**: Status validation prevents invalid state transitions
- **Observability**: Structured logging provides clear audit trails
- **Simplicity**: No over-engineering - straightforward, easy-to-understand code

### Status Management

**StatusTracker Service (`reia/services/status_tracker.py`)**
- Centralized status management for all entities (RiskAssessment, Calculation, CalculationBranch)
- Status transition validation with clear business rules:
  - CREATED → EXECUTING, COMPLETE, FAILED, ABORTED
  - EXECUTING → COMPLETE, FAILED, ABORTED  
  - Terminal states (COMPLETE/FAILED/ABORTED) cannot transition further
- Consistent structured logging for all status changes
- Business logic validation (e.g., RiskAssessment only COMPLETE if both calculations succeed)
- Simple `isinstance()` approach for entity type detection

**Status Workflow:**
1. **Entity Creation**: Default status is CREATED
2. **Processing Start**: Status updated to EXECUTING via StatusTracker
3. **Completion**: Status set to COMPLETE/FAILED/ABORTED based on results
4. **Validation**: Invalid transitions are prevented with clear error messages
5. **Logging**: All changes logged with entity type, ID, old→new status, and reason