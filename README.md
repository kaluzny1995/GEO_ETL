# GEO_ETL

GIS Data ETL Pipeline — PySpark-based extract-transform-load processes for geospatial data.

## Overview

GEO_ETL is the transformation layer in the GIS data pipeline. It reads raw CSV files produced by [GEO_Download](../GEO_Download), applies transformations and enrichment using PySpark, and outputs clean, analysis-ready datasets into Supabase (PostgreSQL).

PySpark scripts normally run inside the `geo_airflow_spark` image and connect to the Spark standalone cluster over the Compose network (`spark://geo-spark-master:7078`). For host-side development they can also run locally against the published master at `spark://localhost:7078`.

## Features

- **Spark standalone cluster** — 1 master + 2 workers via Docker Compose (in GEO_E2E)
- **Shared data access** — CSV files from GEO_Download mounted read-only into all Spark nodes
- **Supabase connectivity** — PostgreSQL JDBC driver for reading/writing to Supabase
- **Custom Spark image** — based on `apache/spark:3.5.5` with Hadoop AWS + PostgreSQL JARs
- **Local PySpark client** — scripts run on the host, connecting to the cluster remotely
- **Model-driven schema** — Pydantic models define table structures; DDL and PySpark schemas are generated automatically
- **Canonical event tables** — the `SINGLE_ETL` job maps every source into one shared 21-column event structure (`geo.<source>_etl`), deduplicates each source to one row per event per `file_date`, and the [`union_etl` script](docs/etl_jobs/single-etl.md#union_etl) stacks them all into one `geo.union_etl` table keyed per source by a distinct `source_name` (see [Data tables](docs/data-tables.md))
- **Views** — the `VIEW` job defines `geo.<name>_vw` views over the `*_etl` tables from queries kept in `config.json` (HISTORICAL only — every run re-proves the query and replaces the view)
- **Data quality metrics** — per-`file_date` null/bad/duplicate rates plus field varyingness (min/max/range, mean/stddev, percentiles, distinct counts) in `geo.data_quality`, measured incrementally (HISTORICAL first, then only new dates); each run also refreshes the whole-table `overall` snapshot
- **CLI entry point** — `run_etl.py` dispatches jobs (CREATE TABLE, INGESTION, DATA_QUALITY, SINGLE_ETL, VIEW) via a job registry

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
| [ETL jobs](docs/etl-jobs.md) | Job registry and links to the CREATE, INGESTION, DATA_QUALITY, SINGLE_ETL and VIEW pipelines, plus load type detection. |
| [Data quality metrics](docs/data-quality-metrics.md) | Metric families, rules and querying `geo.data_quality`. |
| [Data tables](docs/data-tables.md) | The `geo` schema — raw/plain table pairs, registered models, current data and how to add a table. |
| [Configuration](docs/configuration.md) | `config.json` reference and Supabase credentials (`.env`). |
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

## Docker

GEO_ETL is baked into the **`geo_airflow_spark`** image by the Compose stack in
[GEO_E2E](../GEO_E2E):

- Runtime files are copied to `/opt/geo/GEO_ETL`; the `geo_etl_311` conda environment
  (PySpark 3.5.5 + `findspark`) lives at `/opt/conda/envs/geo_etl_311`.
- `/opt/geo/GEO_ETL/.env` is refreshed from the Compose `SUPABASE_DB_*` values by
  `airflow-init` on every start — never baked into an image layer.
- Inside the Compose network the Spark master is `spark://geo-spark-master:7078`
  (set through `SPARK_MASTER_URL`); `config.json` keeps `spark://localhost:7078` for
  local runs.
- Airflow and Spark share the `geo_data` volume at `/opt/geo/GEO_Download/data`, so
  drivers and executors see the same CSVs.

```bash
cd ../GEO_E2E
docker compose up -d --build
```

After changing GEO_ETL, rebuild the Airflow/Spark image:

```bash
docker compose build airflow-init
docker compose up -d
```

## Quick start

```bash
# 1. Create the environment (the Supabase credentials file is written by GEO_E2E's setup)
conda env create -f environment.yml
conda activate geo_etl_311
../GEO_E2E/setup.sh   # writes this repo's .env — also when GEO_E2E/.env already exists
#    manual fallback: cp .env.example .env and edit it (see docs/configuration.md)

# 2. Start the Spark cluster (docker-compose lives in GEO_E2E)
cd ../GEO_E2E
docker compose up -d geo-spark-master geo-spark-worker-1 geo-spark-worker-2
cd ../GEO_ETL

# 3. Verify cluster connectivity
python run_pyspark_csv_test.py

# 4. Create the raw + plain tables, ingest a data source and measure its quality
#    (one CREATE per table: <table_name>_raw and <table_name>)
python run_etl.py CREATE <table_name>_raw
python run_etl.py CREATE <table_name>
python run_etl.py INGESTION <table_name>
python run_etl.py DATA_QUALITY <table_name>

# 5. Map the source into the canonical event structure
python run_etl.py CREATE <table_name>_etl
python run_etl.py SINGLE_ETL <table_name>_etl

# 6. Stack every *_etl table into one union table (the union script)
python run_etl.py CREATE <table_name>
python run_etl.py SINGLE_ETL <table_name>
```

Supabase credentials must be provided as environment variables, most conveniently in the gitignored `.env` file in the project root. **Set them up once in [GEO_E2E](../GEO_E2E)** — run `../GEO_E2E/setup.sh`, which writes this project's `.env` with the same five `SUPABASE_DB_*` values it uses for Airflow's metadata DB (manual fallback: copy `.env.example` and fill it in). Details in [Configuration](docs/configuration.md). Full instructions, including verifying the schema, dropping tables, deleting the latest `file_date` and the standalone upload/raw2plain pipelines, are in [Usage](docs/usage.md).

## Related Projects

- **GEO_Download** — downloads raw CSV data from public APIs
- **GEO_E2E** — orchestrates the whole pipeline with an Apache Airflow DAG (`geo_pipeline`), which spawns `run_download.py` and `run_etl.py` as subprocesses; also hosts the `docker-compose.yaml` that runs the Spark cluster this ETL connects to
- **GEO_API** — FastAPI layer serving the finished data (`geo.data_quality` metrics, `geo.union_etl` event counts) from the same Supabase project; shares the five `SUPABASE_DB_*` credentials written by GEO_E2E's `setup.sh`
- **GEO_APP** — React dashboard over GEO_API (data quality & quantity views); no direct coupling to this project
