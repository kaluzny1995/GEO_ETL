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
- **CLI entry point** — `run_etl.py` dispatches jobs (CREATE TABLE, etc.) via a job registry

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
| Environment      | Conda                               |
| Containers       | Docker + Docker Compose             |

## Project Structure

```
GEO_ETL/
├── config.json                       # App config (Spark master, DB schema, JDBC driver)
├── environment.yml                   # Conda environment definition
├── supabase_config.json              # Supabase credentials (not in git)
├── run_etl.py                        # Main CLI entry point for ETL jobs
├── run_pyspark_csv_test.py           # Cluster verification script
├── run_pyspark_supabase_test.py      # Supabase connection test script
│
├── jobs/                             # Job modules (the "what to do" layer)
│   ├── __init__.py                   # Registers jobs in JobFactory
│   ├── registry.py                   # JobFactory: job type -> module registry
│   └── create_table.py              # CREATE TABLE job (DDL from Pydantic models)
│
├── model/                            # Data models and configuration
│   ├── enums.py                      # JobType enum (CREATE)
│   ├── config/
│   │   ├── app_config.py             # AppConfig, SparkConfig, DatabaseConfig
│   │   └── supabase_config.py        # SupabaseConnectionConfig + JDBC helpers
│   └── raw/
│       └── avalanche_org_raw.py      # Pydantic model for avalanche_org_raw table
│
├── utils/                            # Utility modules
│   ├── ddl_generator.py              # Pydantic -> PostgreSQL DDL + JDBC execution
│   └── schema_converter.py           # Pydantic -> PySpark StructType schemas
│
├── spark/
│   ├── Dockerfile                    # Custom Spark image (apache/spark:3.5.5 + JARs)
│   └── jars/
│       ├── hadoop-aws-3.3.4.jar
│       ├── aws-java-sdk-bundle-1.12.262.jar
│       └── postgresql-42.7.4.jar
│
└── test/                             # Unit tests (pytest)
    ├── jobs/
    │   ├── test_create_table.py
    │   └── test_job_factory.py
    ├── model/
    │   ├── config/
    │   │   ├── test_config.py
    │   │   └── test_supabase_config.py
    │   └── raw/
    │       └── test_avalanche_org_raw.py
    └── util/
        ├── test_ddl_generator.py
        └── test_schema_converter.py
```

## Spark JARs

The `spark/jars/` directory is excluded from git (listed in `.gitignore`). After cloning, download the required JARs manually:

```bash
cd spark/jars
curl -fSL -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
curl -fSL -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
curl -fSL -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
```

| JAR | Version | Purpose |
|-----|---------|---------|
| `hadoop-aws` | 3.3.4 | S3A filesystem client (for future S3/MinIO access) |
| `aws-java-sdk-bundle` | 1.12.262 | AWS SDK dependency required by hadoop-aws |
| `postgresql` | 42.7.4 | PostgreSQL JDBC driver for Supabase connectivity |

These JARs are copied into the Spark Docker image during build.

The PostgreSQL JDBC driver must also be available locally for the PySpark driver (which runs on the host). Copy it to the PySpark installation:

```bash
cp spark/jars/postgresql-42.7.4.jar $(python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))")
```

## Prerequisites

- **Conda** — for environment management
- **Java 11+** — required by PySpark
- **Docker** — for running the Spark cluster
- **GEO_Download** — must be a sibling directory (source of raw CSVs)
- **GEO_E2E** — hosts the `docker-compose.yaml` that runs the Spark cluster

## Setup

### 1. Create the Conda environment

```bash
conda env create -f environment.yml
conda activate geo_etl_311
```

### 2. Start the Spark cluster

The cluster is managed by Docker Compose in [GEO_E2E](../GEO_E2E). Make sure the `.env` file there is configured with the correct `GEO_DATA_DIR` path, then:

```bash
cd ../GEO_E2E
docker compose up -d geo_spark_master geo_spark_worker_1 geo_spark_worker_2
```

### 3. Verify cluster connectivity

```bash
conda activate geo_etl_311
python run_pyspark_csv_test.py
```

Expected output:

```
Connected to: spark://localhost:7078
Spark version: 3.5.5
Data path:     /path/to/GEO_Download/data

  Avalanche_ORG                  -> 19 CSV file(s)
  EONET                          -> 18 CSV file(s)
  GDACS                          -> 19 CSV file(s)
  ...
```

### 4. Create tables in Supabase

The `run_etl.py` CLI creates tables in Supabase by generating DDL from Pydantic models:

```bash
conda activate geo_etl_311
python run_etl.py --job CREATE --table_name <table_name>
```

Expected output:

```
Table geo.<table_name> created.
```

**How it works:**

1. Reads config from `config.json` (Spark master, DB schema, JDBC driver) and `supabase_config.json` (credentials)
2. Connects to the Spark cluster
3. Creates the `geo` schema if it doesn't exist
4. Generates `CREATE TABLE` DDL from the Pydantic model registered for `<table_name>`
5. Executes the DDL against Supabase via JDBC

**Currently registered tables:**

| Table Name | Pydantic Model | Description |
|------------|----------------|-------------|
| `avalanche_org_raw` | `AvalancheOrgRaw` | Avalanche forecast data (15 fields) |

To add a new table, create a Pydantic model in `model/raw/`, add it to `TABLE_REGISTRY` in `jobs/create_table.py`, and create a corresponding test file in `test/model/raw/`.

### 5. Verify table creation

```bash
conda activate geo_etl_311
python run_pyspark_supabase_test.py
```

Expected output now includes the `geo` schema with 1 table:

```
+------------------+------+
|schema            |tables|
+------------------+------+
|auth              |27    |
|extensions        |2     |
|geo               |1     |
|information_schema|69    |
|public            |2     |
|realtime          |3     |
|reddit            |9     |
|storage           |8     |
|vault             |2     |
+------------------+------+
```

## Supabase Configuration

Create `supabase_config.json` in the project root with your Supabase credentials:

```json
{
  "username": "postgres.<YOUR-PROJECT-REF>",
  "password": "<YOUR-PASSWORD>",
  "host": "aws-1-eu-west-1.pooler.supabase.com",
  "port": 5432,
  "database": "postgres"
}
```

> **Important:** This file contains database credentials and is excluded from git (listed in `.gitignore`). Never commit it to version control.

### Test Supabase connectivity

```bash
conda activate geo_etl_311
python run_pyspark_supabase_test.py
```

Expected output:

```
Connected to:   spark://localhost:7078
Spark version:  3.5.5
Supabase host:  aws-1-eu-west-1.pooler.supabase.com
JDBC URL:       jdbc:postgresql://aws-1-eu-west-1.pooler.supabase.com:5432/postgres

+------------------+------+
|schema            |tables|
+------------------+------+
|auth              |27    |
|extensions        |2     |
|geo               |1     |
|information_schema|69    |
|public            |2     |
|realtime          |3     |
|reddit            |9     |
|storage           |8     |
|vault             |2     |
+------------------+------+

Done.
```

## Cluster Configuration

| Service | Container | Hostname | Ports | Resources |
|---------|-----------|----------|-------|-----------|
| Spark Master | `geo_spark_master` | `geo-spark-master` | 7078 (cluster), 8083 (Web UI) | — |
| Spark Worker 1 | `geo_spark_worker_1` | — | 8084 (Web UI) | 2 cores, 2g RAM |
| Spark Worker 2 | `geo_spark_worker_2` | — | 8085 (Web UI) | 2 cores, 2g RAM |

### Spark Web UI

Open http://localhost:8083 in your browser to access the Spark Master Web UI. From there you can:

- **Workers** — see all registered workers, their status, cores, and memory
- **Applications** — monitor running and completed Spark applications
- **Jobs** — view job progress, stages, and tasks for each application
- **Executors** — check executor memory usage, GC stats, and shuffle data

Individual worker Web UIs are available at:
- Worker 1: http://localhost:8084
- Worker 2: http://localhost:8085

## Connecting from PySpark

```python
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://localhost:7078") \
    .appName("my_etl_job") \
    .getOrCreate()

# Read CSVs
df = spark.read.csv("/path/to/GEO_Download/data/USGS_Earthquake/*.csv", header=True)
df.show()
```

> **Note:** The data path must be accessible from both the driver (host) and workers (Docker containers). The Docker Compose in GEO_E2E mounts `GEO_DATA_DIR` at the same absolute path on both sides.

## Testing

All tests are pure unit tests with external dependencies (Spark, Supabase) mocked:

```bash
conda activate geo_etl_311
pytest test/ -v
```

**Test structure:**

| Test Suite | What It Tests |
|------------|---------------|
| `test/jobs/test_create_table.py` | CREATE TABLE job logic, schema creation, DDL execution |
| `test/jobs/test_job_factory.py` | Job registry pattern (register, create, clear) |
| `test/model/config/test_config.py` | AppConfig, SparkConfig, DatabaseConfig models |
| `test/model/config/test_supabase_config.py` | SupabaseConnectionConfig, JDBC URL/properties |
| `test/model/raw/test_avalanche_org_raw.py` | AvalancheOrgRaw model validation + Spark schema |
| `test/util/test_ddl_generator.py` | Pydantic -> PostgreSQL DDL generation |
| `test/util/test_schema_converter.py` | Pydantic -> PySpark StructType conversion |

## Data

Downloaded CSV files from [GEO_Download](../GEO_Download) are the input. Current sources:

| Source | Description |
|--------|-------------|
| Avalanche_ORG | Avalanche data |
| EONET | NASA natural events |
| GDACS | Global disasters |
| IBTrACS | Tropical cyclone best-track |
| NIFC | US wildland fires |
| OpenMeteo_Flood | River discharge data |
| USGS_Earthquake | Seismic events |
| USGS_NWIS | US streamflow |
| USGS_Volcano | US volcano alerts |

## Related Projects

- **GEO_Download** — downloads raw CSV data from public APIs
- **GEO_E2E** — orchestrates downloads via Celery + RabbitMQ; also runs the Spark cluster via Docker Compose
