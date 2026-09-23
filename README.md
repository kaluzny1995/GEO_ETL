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
- **CLI entry point** — `run_etl.py` dispatches jobs (CREATE TABLE, INGESTION, etc.) via a job registry

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
├── config.json                       # App config (Spark, database, logging, data paths)
├── environment.yml                   # Conda environment definition
├── supabase_config.json              # Supabase credentials (not in git)
├── run_etl.py                        # Main CLI entry point for ETL jobs
├── run_drop_table.py                 # Drop a table from Supabase
├── run_drop_schema.py                # Drop a schema from Supabase (if empty)
├── run_delete_last_filedate_data.py  # Delete latest file_date from raw + plain tables
├── run_pyspark_csv_test.py           # Cluster verification script
├── run_pyspark_supabase_test.py      # Supabase connection test script
│
├── upload/                           # Data upload scripts
│   ├── __init__.py
│   ├── avalanche_org_upload.py       # Upload Avalanche_ORG CSVs -> Supabase via JDBC
│   ├── eonet_event_upload.py         # Upload EONET CSVs -> Supabase via JDBC
│   ├── gdacs_event_upload.py         # Upload GDACS CSVs -> Supabase via JDBC
│   ├── ibtracs_cyclone_upload.py     # Upload IBTrACS CSVs -> Supabase via JDBC
│   ├── nifc_fire_upload.py           # Upload NIFC CSVs -> Supabase via JDBC
│   ├── openmeteo_flood_upload.py     # Upload OpenMeteo_Flood CSVs -> Supabase via JDBC
│   └── usgs_earthquake_upload.py     # Upload USGS_Earthquake CSVs -> Supabase via JDBC
│
├── raw2plain/                        # Raw-to-final transformation scripts
│   ├── __init__.py
│   ├── avalanche_org_raw2plain.py    # Transform Avalanche_ORG raw -> typed data
│   ├── eonet_event_raw2plain.py      # Transform EONET raw -> typed data
│   ├── gdacs_event_raw2plain.py      # Transform GDACS raw -> typed data
│   ├── ibtracs_cyclone_raw2plain.py  # Transform IBTrACS raw -> typed data
│   ├── nifc_fire_raw2plain.py        # Transform NIFC raw -> typed data
│   ├── openmeteo_flood_raw2plain.py  # Transform OpenMeteo_Flood raw -> typed data
│   └── usgs_earthquake_raw2plain.py  # Transform USGS_Earthquake raw -> typed data
│
├── factory/                          # Registry factories
│   ├── __init__.py                   # Exports all factories (lazy imports)
│   ├── job_factory.py                # JobFactory: job type -> module registry
│   ├── table_factory.py              # TableFactory: table_name -> Pydantic model
│   ├── upload_script_factory.py      # UploadScriptFactory: table_name -> upload function
│   └── raw2plain_script_factory.py   # Raw2PlainScriptFactory: table_name -> raw2plain function
│
├── jobs/                             # Job modules (the "what to do" layer)
│   ├── __init__.py                   # Registers jobs in JobFactory
│   ├── create_table.py              # CREATE TABLE job (DDL from Pydantic models)
│   └── ingestion.py                 # INGESTION job (upload + raw2plain via factories)
│
├── model/                            # Data models and configuration
│   ├── enums.py                      # JobType enum (CREATE, INGESTION)
│   ├── config/
│   │   ├── app_config.py             # AppConfig, SparkConfig, DatabaseConfig, LoggingConfig, DataConfig
│   │   └── supabase_config.py        # SupabaseConnectionConfig + JDBC helpers
│   ├── raw/
│   │   ├── avalanche_org_raw.py      # AvalancheOrgRaw — all-TEXT raw model
│   │   ├── eonet_event_raw.py        # EonetEventRaw — all-TEXT raw model
│   │   ├── gdacs_event_raw.py        # GdacsEventRaw — all-TEXT raw model
│   │   ├── ibtracs_cyclone_raw.py    # IbtacsCycloneRaw — all-TEXT raw model
│   │   ├── nifc_fire_raw.py          # NifcFireRaw — all-TEXT raw model
│   │   ├── openmeteo_flood_raw.py    # OpenMeteoFloodRaw — all-TEXT raw model
│   │   └── usgs_earthquake_raw.py    # UsgsEarthquakeRaw — all-TEXT raw model
│   └── plain/
│       ├── avalanche_org.py          # AvalancheOrg — typed model (int, bool, datetime, date)
│       ├── eonet_event.py            # EonetEvent — typed model (float, int, datetime, date)
│       ├── gdacs_event.py            # GdacsEvent — typed model (float, int, datetime, bool)
│       ├── ibtracs_cyclone.py        # IbtacsCyclone — typed model (float, int, datetime)
│       ├── nifc_fire.py              # NifcFire — typed model (float, int, datetime)
│       ├── openmeteo_flood.py        # OpenMeteoFlood — typed model (float, date)
│       └── usgs_earthquake.py        # UsgsEarthquake — typed model (float, int, datetime)
│
├── utils/                            # Utility modules
│   ├── ddl_generator.py              # Pydantic -> PostgreSQL DDL + JDBC execution
│   ├── logger.py                     # setup_logger() for file-based logging
│   └── schema_converter.py           # Pydantic -> PySpark StructType schemas
│
├── logs/                             # Runtime log output (gitignored)
│   ├── create_table/                 # CREATE TABLE job logs
│   ├── drop_table/                   # Drop table job logs
│   ├── drop_schema/                  # Drop schema job logs
│   ├── upload/                       # Upload job logs
│   ├── raw2plain/                    # Raw2Plain transform logs
│   ├── pyspark_csv_test/             # CSV test script logs
│   └── pyspark_supabase_test/        # Supabase test script logs
│
├── spark_cluster_image/              # Docker image for Spark cluster
│   ├── Dockerfile                    # Custom Spark image (apache/spark:3.5.5 + JARs)
│   └── jars/
│       ├── hadoop-aws-3.3.4.jar
│       ├── aws-java-sdk-bundle-1.12.262.jar
│       └── postgresql-42.7.4.jar
│
└── test/                             # Unit tests (pytest)
    ├── factory/
    │   ├── test_job_factory.py
    │   ├── test_table_factory.py
    │   ├── test_upload_script_factory.py
    │   └── test_raw2plain_script_factory.py
    ├── jobs/
    │   ├── test_create_table.py
    │   └── test_ingestion.py
    ├── model/
    │   ├── test_enums.py
    │   ├── config/
    │   │   ├── test_config.py
    │   │   └── test_supabase_config.py
    │   ├── plain/
    │   │   ├── test_avalanche_org.py
    │   │   ├── test_eonet_event.py
    │   │   ├── test_gdacs_event.py
    │   │   ├── test_ibtracs_cyclone.py
    │   │   ├── test_nifc_fire.py
    │   │   ├── test_openmeteo_flood.py
    │   │   └── test_usgs_earthquake.py
    │   └── raw/
    │       ├── test_avalanche_org_raw.py
    │       ├── test_eonet_event_raw.py
    │       ├── test_gdacs_event_raw.py
    │       ├── test_ibtracs_cyclone_raw.py
    │       ├── test_nifc_fire_raw.py
    │       ├── test_openmeteo_flood_raw.py
    │       └── test_usgs_earthquake_raw.py
    ├── scripts/
    │   ├── test_run_etl.py
    │   ├── test_run_drop_table.py
    │   ├── test_run_drop_schema.py
    │   └── test_run_delete_last_filedate_data.py
    ├── upload/
    │   ├── test_avalanche_org_upload.py
    │   ├── test_eonet_event_upload.py
    │   ├── test_gdacs_event_upload.py
    │   ├── test_ibtracs_cyclone_upload.py
    │   ├── test_nifc_fire_upload.py
    │   ├── test_openmeteo_flood_upload.py
    │   └── test_usgs_earthquake_upload.py
    ├── raw2plain/
    │   ├── test_avalanche_org_raw2plain.py
    │   ├── test_eonet_event_raw2plain.py
    │   ├── test_gdacs_event_raw2plain.py
    │   ├── test_ibtracs_cyclone_raw2plain.py
    │   ├── test_nifc_fire_raw2plain.py
    │   ├── test_openmeteo_flood_raw2plain.py
    │   └── test_usgs_earthquake_raw2plain.py
    └── util/
        ├── test_ddl_generator.py
        ├── test_logger.py
        └── test_schema_converter.py
```

## Spark JARs

The `spark_cluster_image/jars/` directory is excluded from git (listed in `.gitignore`). After cloning, download the required JARs manually:

```bash
cd spark_cluster_image/jars
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
cp spark_cluster_image/jars/postgresql-42.7.4.jar $(python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))")
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
python run_etl.py CREATE <table_name>
python run_etl.py INGESTION <table_name>
```

Expected output:

```
Table geo.<table_name> created.
```

**How it works:**

1. Reads config from `config.json` (Spark master, DB schema, JDBC driver) and `supabase_config.json` (credentials)
2. Connects to the Spark cluster
3. For `CREATE`: creates the schema if needed, generates DDL from the Pydantic model, executes it against Supabase
4. For `INGESTION`: runs the upload script (CSVs → raw table) then the raw2plain script (raw → typed table) via factories

**Currently registered tables:**

| Table Name | Pydantic Model | Location | Description |
|------------|----------------|----------|-------------|
| `avalanche_org_raw` | `AvalancheOrgRaw` | `model/raw/` | Avalanche forecast data — all fields as TEXT (15 fields) |
| `avalanche_org` | `AvalancheOrg` | `model/plain/` | Avalanche forecast data — typed fields (int, bool, datetime, date) |

To add a new table, create a Pydantic model in `model/raw/` (all-TEXT) or `model/plain/` (typed), register it in `model/__init__.py` using `TableFactory.register()`, and create a corresponding test file.

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

### 6. Drop a table

```bash
conda activate geo_etl_311
python run_drop_table.py <table_name>
```

Expected output:

```
Table geo.<table_name> dropped.
```

### 7. Drop a schema

Only works if the schema contains no tables:

```bash
conda activate geo_etl_311
python run_drop_schema.py <schema_name>
```

Expected output (empty schema):

```
Schema '<schema_name>' dropped.
```

If the schema has tables:

```
Schema '<schema_name>' contains tables. Drop tables first.
```

### 8. Delete latest file_date data

Delete all rows with the most recent `file_date` from both raw and plain tables. Useful for re-running INCREMENTAL tests:

```bash
conda activate geo_etl_311
python run_delete_last_filedate_data.py <raw_table_name>
```

The script derives the plain table name automatically by stripping the `_raw` suffix (e.g. `avalanche_org_raw` → `avalanche_org`).

Expected output:

```
Latest file_date: 2026-09-22
Deleted 166 rows from geo.avalanche_org
Deleted 166 rows from geo.avalanche_org_raw
Delete complete: file_date=2026-09-22, raw=166, plain=166
```

### 9. Upload data to Supabase

Upload CSV files from GEO_Download to a Supabase table:

```python
from pyspark.sql import SparkSession
from model.config import AppConfig, SupabaseConnectionConfig
from upload.avalanche_org_upload import upload

config = AppConfig.from_json()
db_config = SupabaseConnectionConfig.from_json()
spark = SparkSession.builder.master("spark://localhost:7078").appName("upload").getOrCreate()

upload(spark, config, db_config)
spark.stop()
```

**Load type is determined automatically:**

| Condition | Load Type | Behavior |
|-----------|-----------|----------|
| Table is empty | `HISTORICAL` | Uploads all CSV files |
| Table has data | `INCREMENTAL` | Uploads only CSVs with `file_date` newer than the latest in the table |

When called from the INGESTION job, a logger is passed so all logs go to the same file:

```python
upload(spark, config, db_config, logger=my_logger)
```

Expected output:

```
---- Upload started: avalanche_org_raw (HISTORICAL) ----
Data directory: /path/to/GEO_Download/data/Avalanche_ORG
HISTORICAL: reading all 19 CSV files
Total records to upload: 1523
Writing to geo.avalanche_org_raw...
Upload complete: 1523 records written to geo.avalanche_org_raw
Spark session stopped
```

When no new files are found (INCREMENTAL and table is up to date), the script raises an error:

```
logger.warning("No new files to upload for avalanche_org_raw")
ValueError: No new files to upload for avalanche_org_raw
```

#### Incremental test workflow

1. Run HISTORICAL ingestion to load all data
2. Delete latest `file_date` from both raw + plain tables using `run_delete_last_filedate_data.py`
3. Run INGESTION again — script detects missing files and re-loads the deleted data

### 9. Transform raw data to final types

Convert data from `avalanche_org_raw` (all TEXT) to `avalanche_org` (typed: int, bool, datetime, date):

```python
from pyspark.sql import SparkSession
from model.config import AppConfig, SupabaseConnectionConfig
from raw2plain.avalanche_org_raw2plain import raw2plain

config = AppConfig.from_json()
db_config = SupabaseConnectionConfig.from_json()
spark = SparkSession.builder.master("spark://localhost:7078").appName("raw2plain").getOrCreate()

raw2plain(spark, config, db_config)
spark.stop()
```

**Load type is determined automatically:**

| Condition | Load Type | Behavior |
|-----------|-----------|----------|
| Final table is empty | `HISTORICAL` | Processes all raw data |
| Final table has data | `INCREMENTAL` | Processes only raw rows with `file_date` > max in final |

When called from the INGESTION job, a logger is passed so all logs go to the same file:

```python
raw2plain(spark, config, db_config, logger=my_logger)
```

**Type conversions applied:**

| Column | Raw Type | Final Type |
|--------|----------|------------|
| `zone_id` | TEXT | INTEGER |
| `danger_level` | TEXT | INTEGER |
| `off_season` | TEXT | BOOLEAN |
| `forecast_start` | TEXT | TIMESTAMP |
| `forecast_end` | TEXT | TIMESTAMP |
| `date` | TEXT | DATE |

## Application Configuration

All runtime settings are in `config.json`:

```json
{
  "spark": {
    "master": "spark://localhost:7078",
    "app_name_prefix": "geo_etl"
  },
  "database": {
    "db_schema": "geo",
    "jdbc_driver": "org.postgresql.Driver"
  },
  "logging": {
    "directory": "logs",
    "level": "INFO"
  },
  "data": {
    "directory": "../GEO_Download/data"
  }
}
```

| Section | Key | Default | Description |
|---------|-----|---------|-------------|
| `spark.master` | — | — | Spark cluster master URL |
| `spark.app_name_prefix` | — | `"geo_etl"` | Prefix for Spark application names |
| `database.db_schema` | — | — | PostgreSQL schema name |
| `database.jdbc_driver` | — | — | JDBC driver class |
| `logging.directory` | `"logs"` | — | Root directory for log files |
| `logging.level` | `"INFO"` | — | Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL |
| `data.directory` | `"../GEO_Download/data"` | — | Relative path to CSV data source |

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
| `test/factory/test_job_factory.py` | JobFactory (register, create, clear) |
| `test/factory/test_table_factory.py` | TableFactory (register, get, tables, clear) |
| `test/factory/test_upload_script_factory.py` | UploadScriptFactory (register, get, tables, clear) |
| `test/factory/test_raw2plain_script_factory.py` | Raw2PlainScriptFactory (register, get, tables, clear) |
| `test/jobs/test_create_table.py` | CREATE TABLE job logic, schema creation, DDL execution |
| `test/jobs/test_ingestion.py` | INGESTION job (upload + raw2plain ordering, config passing, logger passing, logging) |
| `test/model/config/test_config.py` | AppConfig, SparkConfig, DatabaseConfig models |
| `test/model/config/test_supabase_config.py` | SupabaseConnectionConfig, JDBC URL/properties |
| `test/model/test_enums.py` | JobType enum (values, membership, from_value, invalid value) |
| `test/model/raw/test_avalanche_org_raw.py` | AvalancheOrgRaw model validation + Spark schema |
| `test/model/raw/test_eonet_event_raw.py` | EonetEventRaw model validation + Spark schema |
| `test/model/raw/test_gdacs_event_raw.py` | GdacsEventRaw model validation + Spark schema |
| `test/model/raw/test_ibtracs_cyclone_raw.py` | IbtacsCycloneRaw model validation + Spark schema |
| `test/model/plain/test_avalanche_org.py` | AvalancheOrg model validation + Spark schema + DDL types |
| `test/model/plain/test_eonet_event.py` | EonetEvent model validation + Spark schema + DDL types |
| `test/model/plain/test_gdacs_event.py` | GdacsEvent model validation + Spark schema + DDL types |
| `test/model/plain/test_ibtracs_cyclone.py` | IbtacsCyclone model validation + Spark schema + DDL types |
| `test/upload/test_avalanche_org_upload.py` | Upload script (helpers + upload function: historical, incremental, no files, logging, logger param) |
| `test/upload/test_eonet_event_upload.py` | EONET upload script (helpers + upload function: historical, incremental, no files, logging, logger param) |
| `test/upload/test_gdacs_event_upload.py` | GDACS upload script (helpers + upload function: historical, incremental, no files, logging, logger param) |
| `test/upload/test_ibtracs_cyclone_upload.py` | IBTrACS upload script (helpers + upload function: historical, incremental, no files, logging, logger param) |
| `test/scripts/test_run_etl.py` | run_etl.py (CLI dispatch, Spark session lifecycle, logger passing, error handling) |
| `test/raw2plain/test_avalanche_org_raw2plain.py` | Raw2Plain transform (historical, incremental, no records, type conversion, logger param) |
| `test/raw2plain/test_eonet_event_raw2plain.py` | EONET Raw2Plain transform (historical, incremental, no records, type conversion, logger param) |
| `test/raw2plain/test_gdacs_event_raw2plain.py` | GDACS Raw2Plain transform (historical, incremental, no records, type conversion, logger param) |
| `test/raw2plain/test_ibtracs_cyclone_raw2plain.py` | IBTrACS Raw2Plain transform (historical, incremental, no records, type conversion, logger param) |
| `test/scripts/test_run_drop_table.py` | run_drop_table.py CLI script |
| `test/scripts/test_run_drop_schema.py` | run_drop_schema.py CLI script |
| `test/scripts/test_run_delete_last_filedate_data.py` | run_delete_last_filedate_data.py (delete latest file_date, table derivation, skip on empty, error handling) |
| `test/util/test_ddl_generator.py` | DDL generation, create/execute schema, drop table, drop schema, schema table check |
| `test/util/test_schema_converter.py` | Pydantic -> PySpark StructType conversion |
| `test/util/test_logger.py` | Logger setup, duplicate handlers, nested directories |

## Logging

Every script writes structured log files to `logs/`, organized by job type. The logging setup uses the same pattern as [GEO_E2E](../GEO_E2E) and [GEO_Download](../GEO_Download).

When called via the INGESTION job, `upload()` and `raw2plain()` receive the ingestion logger, so all logs are consolidated into a single ingestion log file. When called standalone, they create their own separate log files.

### Log structure

```
logs/
├── ingestion/
│   └── ingestion_avalanche_org_raw_2026-09-21T10:42:00.777888.log
├── create_table/
│   └── create_table_avalanche_org_raw_2026-09-21T10:30:00.123456.log
├── drop_table/
│   └── drop_table_avalanche_org_raw_2026-09-21T10:35:00.654321.log
├── drop_schema/
│   └── drop_schema_geo_2026-09-21T10:40:00.111222.log
├── delete_last_filedate/
│   └── delete_last_filedate_avalanche_org_raw_2026-09-21T10:42:00.777888.log
├── upload/                          # standalone only
│   └── upload_avalanche_org_raw_2026-09-21T10:42:00.777888.log
├── raw2plain/                       # standalone only
│   └── raw2plain_avalanche_org_2026-09-21T10:45:00.333444.log
├── pyspark_csv_test/
│   └── pyspark_csv_test_2026-09-21T10:45:00.333444.log
└── pyspark_supabase_test/
    └── pyspark_supabase_test_2026-09-21T10:50:00.555666.log
```

### Log format

```
2026-09-21 10:30:00,123 INFO ---- ETL job started: create_table avalanche_org_raw ----
2026-09-21 10:30:00,124 INFO Connected to Spark: spark://localhost:7078
2026-09-21 10:30:00,124 INFO Spark version: 3.5.5
2026-09-21 10:30:00,125 INFO Creating schema 'geo' if not exists...
2026-09-21 10:30:00,125 INFO Schema 'geo' ready
2026-09-21 10:30:00,125 INFO Generated DDL for geo.avalanche_org_raw
2026-09-21 10:30:00,125 INFO Executing DDL against Supabase...
2026-09-21 10:30:00,125 INFO DDL executed successfully
2026-09-21 10:30:00,125 INFO Table geo.avalanche_org_raw created.
2026-09-21 10:30:00,126 INFO Spark session stopped
```

### Logger usage in code

```python
from utils.logger import setup_logger

logger = setup_logger(
    name="my_job",
    log_file=f"logs/my_job/my_job_{dt.datetime.now().isoformat()}.log",
)
logger.info("Job started")
logger.warning("Something looks off")
logger.error("Something failed")
logger.exception("Traceback for failure")  # includes stack trace
```

Each log file is timestamped and unique per execution. The `setup_logger` function auto-creates directories and prevents duplicate handlers.

## Data

Downloaded CSV files from [GEO_Download](../GEO_Download) are the input. Current sources:

| Source | Raw Table | Final Table | Records | Load Type |
|--------|-----------|-------------|---------|-----------|
| Avalanche_ORG | `geo.avalanche_org_raw` | `geo.avalanche_org` | 3,485 | HISTORICAL |
| EONET | `geo.eonet_event_raw` | `geo.eonet_event` | 109 | HISTORICAL |
| GDACS | `geo.gdacs_event_raw` | `geo.gdacs_event` | 8,031 | HISTORICAL |
| IBTrACS | `geo.ibtracs_cyclone_raw` | `geo.ibtracs_cyclone` | 424 | HISTORICAL |
| NIFC | `geo.nifc_fire_raw` | `geo.nifc_fire` | 128 | HISTORICAL |
| OpenMeteo_Flood | `geo.openmeteo_flood_raw` | `geo.openmeteo_flood` | 4,884 | HISTORICAL |
| USGS_Earthquake | `geo.usgs_earthquake_raw` | `geo.usgs_earthquake` | 7,084 | HISTORICAL |
| USGS_Volcano | `geo.usgs_volcano_raw` | `geo.usgs_volcano` | 24 | HISTORICAL |
| USGS_NWIS | `geo.usgs_nwis_raw` | `geo.usgs_nwis` | 36 | HISTORICAL |
| USGS_Volcano | — | — | — | — |

## Related Projects

- **GEO_Download** — downloads raw CSV data from public APIs
- **GEO_E2E** — orchestrates downloads via Celery + RabbitMQ; also runs the Spark cluster via Docker Compose
