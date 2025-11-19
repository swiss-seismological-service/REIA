[![codecov](https://codecov.io/gh/swiss-seismological-service/REIA/graph/badge.svg?token=HDUMCZ0VLK)](https://codecov.io/gh/swiss-seismological-service/REIA)

# Rapid Earthquake Impact Assessment (REIA)

## Overview
REIA works as an orchestrator to run seismic risk assessments using the OpenQuake engine. It stores and manages data in a PostgreSQL database, and allows for easy access via a webservice.

It provides the user with a structured workflow for configuring and running seismic risk assessments and evaluating the results. Reproducibility, persistence, and traceability of data and results are built into the software.

REIA is developed by the Swiss Seismological Service (SED) at ETH Zurich as part of the [Swiss National Earthquake Risk Model (ERM-CH)](http://seismo.ethz.ch/en/research-and-teaching/projects/erm-ch23/) project. It is operational in Switzerland to provide rapid earthquake impact assessments ([example](http://seismo.ethz.ch/en/earthquakes/switzerland/event-ria/index.html?originId=%27c21pOmNoLmV0aHouc2VkL3NjMjBhZy9PcmlnaW4vTkxMLjIwMjUwOTAyMjEzMzQ3LjA2NjAzNC4xMzU2MTM=%27&date_ch=2025-09-02&time_ch=09:49&region=Strada%20GR&magnitude=3.8)) after significant seismic events. Its detailed results are distributed to civil protection authorities and other stakeholders to support emergency response and recovery efforts.  
The software is also used for [scenarios](http://seismo.ethz.ch/en/earthquake-country-switzerland/earthquake-scenarios/) in research projects, civil protection planning, and insurance applications.

## Components
The REIA software consists of four main components:
1. A PostgreSQL database with [PostGIS](https://postgis.net/) and [pg_weighted_statistics](https://gitlab.seismo.ethz.ch/erm-ch/pg-weighted-statistics) extensions
2. An [OpenQuake Engine](https://github.com/gem/oq-engine) instance
3. A webservice to access the data
4. This repository, containing the `REIA CLI`, configuration examples and base data.

## Installation

### Prerequisites

- **Python ≥3.10** *(optimally including python3.\*-venv and python3.\*-dev packages)*
- **Docker** *(official installation from [https://docs.docker.com/engine/install/](https://docs.docker.com/engine/install/))*

### Installing the Services using Docker Compose

First, clone the repository:

```bash
git clone https://github.com/swiss-seismological-service/REIA.git
cd REIA
```

Then, copy the example environment file, which contains the necessary settings and credentials. A list with descriptions can be found [here](./docs/configurations.md). The file can be left as is for a local setup, for a server installation you should edit the variables accordingly.  
With a valid `.env` file, you can start the services using Docker Compose:

```bash
cp .env.example .env
docker compose up -d
```

### Installing the CLI

All operations are performed via the `REIA CLI` tool. It can be installed in a virtual environment as follows:

```bash
python -m venv env
source env/bin/activate
pip install -e .
reia --help
```

### Get started

After those installation steps, you can start using the REIA CLI to manage your data and run risk assessments. The first thing you need to do is to set up the database schema:

```bash
reia db migrate
```

Now you can add models, run calculations and risk assessments. Please refer to the [Calculations](./docs/calculations.md) document for more details.

