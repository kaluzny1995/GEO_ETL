← [README](../README.md)

# ETL Jobs

`run_etl.py` is the main CLI entry point. It dispatches jobs through a registry, so adding a new job type means registering a module in `factory/job_factory.py` — no changes to the CLI itself.

## Job registry and dispatch

`JobFactory` (`factory/job_factory.py`) maps a job type from the `JobType` enum (`model/enums.py`) to its module in `jobs/`:

| Job Type | Module | What it does |
|----------|--------|--------------|
| `CREATE` | `jobs/create_table.py` | Creates the schema and the tables from Pydantic DDL |
| `INGESTION` | `jobs/ingestion.py` | Upload CSVs → raw table, then raw → typed table |

Every run:

1. Reads `config.json` (Spark master, DB schema, JDBC driver) and `supabase_config.json` (credentials).
2. Creates a Spark session connected to the cluster and registers the stage memory logger (see [Logging](logging.md#stage-memory-monitoring)).
3. Resolves the job type to its module and executes it, passing the table name, config objects and logger.
4. Stops the Spark session.

```bash
python run_etl.py CREATE avalanche_org
python run_etl.py INGESTION avalanche_org
```

## CREATE TABLE job

`jobs/create_table.py`:

1. Creates the target schema (`geo` by default) if it does not exist.
2. Looks up the Pydantic model for the table in `TableFactory`.
3. Generates `CREATE TABLE` DDL from the model (`utils/ddl_generator.py`) — the raw model produces all-TEXT columns, the plain model produces typed columns.
4. Executes the DDL against Supabase over JDBC.

Spark schemas are derived from the same models via `utils/schema_converter.py` (Pydantic → `StructType`), so DDL, Spark schema and validation always agree.

```
INFO Creating schema 'geo' if not exists...
INFO Schema 'geo' ready
INFO Generated DDL for geo.avalanche_org_raw
INFO Executing DDL against Supabase...
INFO Table geo.avalanche_org_raw created.
```

## INGESTION job

`jobs/ingestion.py` runs the two pipelines in order via the factories:

1. **Upload** — `UploadScriptFactory` picks the upload script for the table; CSVs from `data.directory` are written to the raw table.
2. **Raw2Plain** — `Raw2PlainScriptFactory` picks the transform; typed rows are written to the plain table.

Both steps receive the same config objects and the ingestion logger, so everything lands in one `logs/ingestion/ingestion_<table>_<timestamp>.log`. If either step reports no new data, the job finishes successfully without writing.

Details of each step: [Upload pipeline](#upload-pipeline) · [Raw2Plain pipeline](#raw2plain-pipeline).

## Load type detection

Both pipelines decide their load type from the state of the target table — no manual flag is needed.

### Upload

| Condition | Load Type | Behavior |
|-----------|-----------|----------|
| Table is empty | `HISTORICAL` | Uploads all CSV files |
| Table has data | `INCREMENTAL` | Uploads only CSVs with `file_date` newer than the latest in the table |

### Raw2Plain

| Condition | Load Type | Behavior |
|-----------|-----------|----------|
| Final table is empty | `HISTORICAL` | Processes all raw data |
| Final table has data | `INCREMENTAL` | Processes only raw rows with `file_date` > max in final |

When nothing new is found, both steps log a warning and return without writing — the script still finishes successfully:

```
logger.warning("No new files to upload for avalanche_org_raw — nothing to do, table is up to date")
logger.warning("No new records to process for avalanche_org_raw — nothing to do, table is up to date")
```

To exercise the INCREMENTAL path, follow the [incremental test workflow](usage.md#incremental-test-workflow).

## Upload pipeline

Upload CSV files from GEO_Download to a Supabase table (`upload/` scripts are thin wrappers around `utils/upload_utils.py`):

```python
from pyspark.sql import SparkSession
from model.config import AppConfig, SupabaseConnectionConfig
from upload.avalanche_org_upload import upload

config = AppConfig.from_json()
db_config = SupabaseConnectionConfig.from_json()
spark = SparkSession.builder.master("spark://localhost:7078").appName("upload").getOrCreate()

upload(spark, config, db_config, "avalanche_org_raw", AvalancheOrgRaw, "Avalanche_ORG")
spark.stop()
```

When called from the INGESTION job, a logger is passed so all logs go to the same file:

```python
upload(spark, config, db_config, logger=my_logger)
```

Expected output:

```
---- Upload started: avalanche_org_raw (HISTORICAL) ----
Data directory: /path/to/GEO_Download/data/Avalanche_ORG
HISTORICAL: reading all 19 CSV files
Writing to geo.avalanche_org_raw...
Upload complete: 19 files written to geo.avalanche_org_raw
Spark session stopped
```

## Raw2Plain pipeline

Convert data from `avalanche_org_raw` (all TEXT) to `avalanche_org` (typed: int, bool, datetime, date) (`raw2plain/` scripts are thin wrappers around `utils/raw2plain_utils.py`):

```python
from pyspark.sql import SparkSession
from model.config import AppConfig, SupabaseConnectionConfig
from raw2plain.avalanche_org_raw2plain import raw2plain

config = AppConfig.from_json()
db_config = SupabaseConnectionConfig.from_json()
spark = SparkSession.builder.master("spark://localhost:7078").appName("raw2plain").getOrCreate()

raw2plain(spark, config, db_config, "avalanche_org_raw", "avalanche_org", convert_fn)
spark.stop()
```

When called from the INGESTION job, a logger is passed so all logs go to the same file:

```python
raw2plain(spark, config, db_config, logger=my_logger)
```

**Type conversions applied (Avalanche_ORG):**

| Column | Raw Type | Final Type |
|--------|----------|------------|
| `zone_id` | TEXT | INTEGER |
| `danger_level` | TEXT | INTEGER |
| `off_season` | TEXT | BOOLEAN |
| `forecast_start` | TEXT | TIMESTAMP |
| `forecast_end` | TEXT | TIMESTAMP |
| `date` | TEXT | DATE |

## Memory Optimization

The upload and raw2plain pipelines are optimized for memory efficiency:

### Upload (`utils/upload_utils.py`)
- **No pre-count**: Removed `df.count()` before write — avoids forcing full DataFrame materialization just for a log message
- **Incremental union**: Builds the union chain without storing intermediates in a list, allowing GC to collect frames as they're merged
- **Schema hoisted**: `pydantic_to_spark_schema()` called once outside the CSV loop instead of per-file

### Raw2Plain (`utils/raw2plain_utils.py`)
- **Lightweight empty check**: `raw_df.head(1)` instead of `raw_df.count()` — avoids full table scan just to check if data exists

### Design Decisions
- Upload and raw2plain each read from JDBC once (no double scans)
- Pydantic models are lightweight at import time (~KB each); eager loading is acceptable
- `findspark.init()` is idempotent; called at module import for simplicity
