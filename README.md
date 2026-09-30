# GEO_ETL

GIS Data ETL Pipeline — PySpark-based extract-transform-load processes for geospatial data.

## Overview

GEO_ETL is the transformation layer in the GIS data pipeline. It reads raw CSV files produced by [GEO_Download](../GEO_Download), applies transformations and enrichment using PySpark, and outputs clean, analysis-ready datasets into Supabase (PostgreSQL).

PySpark scripts run locally and connect to a Spark standalone cluster managed by Docker Compose (configured in [GEO_E2E](../GEO_E2E)). The cluster exposes a master at `spark://localhost:7078` with two workers.

## Features

- **Spark standalone cluster** — 1 master + 2 workers via Docker Compose (in GEO_E2E)
- **Shared data access** — CSV files from GEO_Download mounted read-only into all Spark nodes
- **Supabase connectivity** — PostgreSQL JDBC driver for reading/writing to Supabase
- **Custom Spark image** — based on `apache/spark:3.5.5` with Hadoop AWS + PostgreSQL JARs
- **Local PySpark client** — scripts run on the host, connecting to the cluster remotely
- **Model-driven schema** — Pydantic models define table structures; DDL and PySpark schemas are generated automatically
- **Canonical event tables** — the `SINGLE_ETL` job maps every source into one shared 23-column event structure (`geo.<source>_etl`), so the datasets can be merged (see [Data tables](docs/data-tables.md))
- **Data quality metrics** — per-`file_date` null/bad/duplicate rates plus field varyingness (min/max/range, mean/stddev, percentiles, distinct counts) in `geo.data_quality`, measured incrementally (HISTORICAL first, then only new dates)
- **CLI entry point** — `run_etl.py` dispatches jobs (CREATE TABLE, INGESTION, DATA_QUALITY, SINGLE_ETL) via a job registry

## Tech Stack

| Component        | Technology                          |
|------------------|-------------------------------------|
| Language         | Python 3.11                         |
| Processing       | PySpark 3.5.5                       |
| Cluster          | Apache Spark 3.5.5 standalone       |
| Database         | PostgreSQL (Supabase) via JDBC      |
| Data Validation  | Pydantic >= 2.0.0                   |
| HTTP Client      | requests >= 2.31.0                  |
| Testing          | pytest >= 7.0.0                     |
| Linting/Format   | ruff >= 0.6 (lint + format + import sort) |
| Check runner     | tox >= 4.0 (isolated lint/test/typecheck) |
| Type checking    | mypy >= 1.10                        |
| Environment      | Conda                               |
| Containers       | Docker + Docker Compose             |

## Documentation

| Document | Contents |
|----------|----------|
| [Project structure](docs/architecture.md) | Directory layout — what each folder and entry-point script is for. |
| [Usage](docs/usage.md) | Setup, creating tables, running ingestion, drop/delete scripts and expected output. |
| [ETL jobs](docs/etl-jobs.md) | Job registry and links to the CREATE, INGESTION, DATA_QUALITY and SINGLE_ETL pipelines, plus load type detection. |
| [Data quality metrics](docs/data-quality-metrics.md) | Metric families, rules and querying `geo.data_quality`. |
| [Data tables](docs/data-tables.md) | The `geo` schema — raw/plain table pairs, registered models, current data and how to add a table. |
| [Configuration](docs/configuration.md) | `config.json` and `supabase_config.json` reference. |
| [Spark cluster](docs/spark-cluster.md) | Cluster services, Web UI, Spark JARs and connecting from PySpark. |
| [Logging](docs/logging.md) | Log structure and format, plus stage memory monitoring and the SparkListener build. |
| [Testing](docs/testing.md) | Running pytest and what each test suite covers. |
| [Code quality](docs/development.md) | Ruff lint/format, mypy and the tox environments. |

## Prerequisites

- **Conda** — for environment management
- **Java 11+** — required by PySpark
- **Docker** — for running the Spark cluster
- **GEO_Download** — must be a sibling directory (source of raw CSVs)
- **GEO_E2E** — hosts the `docker-compose.yaml` that runs the Spark cluster

## Quick start

```bash
# 1. Create the environment
conda env create -f environment.yml
conda activate geo_etl_311

# 2. Start the Spark cluster (docker-compose lives in GEO_E2E)
cd ../GEO_E2E
docker compose up -d geo_spark_master geo_spark_worker_1 geo_spark_worker_2
cd ../GEO_ETL

# 3. Verify cluster connectivity
python run_pyspark_csv_test.py

# 4. Create tables, ingest a data source and measure its quality
python run_etl.py CREATE avalanche_org
python run_etl.py INGESTION avalanche_org
python run_etl.py DATA_QUALITY avalanche_org

# 5. Map the source into the canonical event structure
python run_etl.py CREATE avalanche_org_etl
python run_etl.py SINGLE_ETL avalanche_org_etl
```

Supabase credentials must be present in `supabase_config.json` — see [Configuration](docs/configuration.md). Full instructions, including verifying the schema, dropping tables, deleting the latest `file_date` and the standalone upload/raw2plain pipelines, are in [Usage](docs/usage.md).

## Related Projects

- **GEO_Download** — downloads raw CSV data from public APIs
- **GEO_E2E** — orchestrates downloads via Celery + RabbitMQ; also runs the Spark cluster via Docker Compose
