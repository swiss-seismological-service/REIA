[![codecov](https://codecov.io/gh/swiss-seismological-service/REIA/graph/badge.svg?token=HDUMCZ0VLK)](https://codecov.io/gh/swiss-seismological-service/REIA)

# Rapid Earthquake Impact Assessment

REIA is a tool for rapid earthquake impact assessment that integrates with OpenQuake engine for seismic risk calculations.

## Quick Start

### Prerequisites

- Python ≥3.10, ≤3.12
- Docker and Docker Compose
- PostgreSQL (if not using Docker)
- **pg_weighted_statistics extension** (automatically installed with Docker)

### Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd REIA
   ```

2. **Set up environment**:
   ```bash
   # Create virtual environment
   python -m venv env
   source env/bin/activate  # On Windows: env\Scripts\activate
   
   # Install REIA
   pip install -e .[dev]
   ```

3. **Configure environment variables**:
   ```bash
   # Copy example environment file
   cp .env.example .env
   # Edit .env with your settings
   ```

### Running with Docker Compose (Recommended)

1. **Configure environment**:
   ```bash
   cp .env.example .env
   # Edit .env with your preferred settings
   ```

2. **Start services**:
   ```bash
   docker-compose up -d --build
   ```

3. **Set up database schema**:
   ```bash
   # This must be run manually after Docker services are up
   reia db migrate
   ```

### Running without Docker

1. **Set up PostgreSQL** with PostGIS extension
2. **Install pg_weighted_statistics extension**:
   - Follow installation instructions at: https://github.com/schmidni/pg_weighted_statistics
   - Or install via PGXN: `pgxn install pg_weighted_statistics`
3. **Create Extensions** in your database:
   ```sql
   CREATE EXTENSION postgis;
   CREATE EXTENSION weighted_statistics;
   ```
4. **Configure `.env`** with your database connection details
5. **Run migrations**:
   ```bash
   reia db migrate
   ```

### Run Tests

```bash
pip install -e .[dev]
pytest --cov=.
```

## Services

When using Docker Compose, the following services are available:

- **PostgreSQL**: Database with PostGIS and pg_weighted_statistics extensions (port 5432)
- **OpenQuake**: Seismic hazard and risk calculations (port 8800)

## Environment Variables

See `.env.example` for all available configuration options.

## Usage

### Database Operations
```bash
# Database migrations
reia db migrate          # Run database migrations
reia db current          # Show current migration
reia db history          # Show migration history
reia db downgrade <revision>    # Rollback to previous migration
reia db downgrade -- -1 # Rollback to by 1 migration
reia db downgrade base  # Remove all Tables, Functions and Triggers
```

### Data Management
```bash
# Add models from files
reia exposure add <file> <name>         # Add exposure model
reia vulnerability add <file> <name>    # Add vulnerability model
reia fragility add <file> <name>        # Add fragility model
reia taxonomymap add <file> <name>      # Add taxonomy mapping

# List existing models
reia exposure list                      # List exposure models
reia vulnerability list                 # List vulnerability models
reia fragility list                     # List fragility models

# Export models to files
reia exposure create_file <id> <path>   # Export exposure model
reia vulnerability create_file <id> <path>  # Export vulnerability model
```

### Risk Assessment
```bash
# Run complete risk assessment
reia risk-assessment run <origin_id> --loss <loss_settings> --damage <damage_settings>

# Manage risk assessments
reia risk-assessment list               # List all risk assessments
reia risk-assessment delete <id>        # Delete risk assessment

# Individual calculations
reia calculation run --settings <file1> <file2> --weights <w1> <w2>
reia calculation list                   # List all calculations
```

### Example Workflow
```bash
# 1. Start services
docker-compose up -d --build

# 2. Set up database
reia db migrate

# 3. Add models
reia exposure add data/exposure.xml "Exposure_model_1"
reia vulnerability add data/vulnerability.xml "Vulnerability_model_1"
reia fragility add data/fragility.xml "Fragility_model_1"

# 4. Run risk assessment
reia risk-assessment run "scenario_1" --loss settings/loss.ini --damage settings/damage.ini

# 5. Check results
reia risk-assessment list
```
