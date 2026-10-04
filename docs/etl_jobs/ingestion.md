← [ETL jobs](../etl-jobs.md)

# INGESTION job

`jobs/ingestion.py` runs the two pipelines in order via the factories — CSVs land in the raw table, typed rows in the plain table:

```mermaid
flowchart TD
    CLI["CLI<br/>python run_etl.py INGESTION avalanche_org"] -->|JobFactory, _raw suffix| JOB["jobs/ingestion.py"]
    JOB --> FU["UploadScriptFactory.get<br/>upload(spark, config, db_config, logger)"]
    JOB --> FT["Raw2PlainScriptFactory.get<br/>raw2plain(spark, config, db_config, logger)"]

    subgraph UP["Step 1 — Upload (ingestion/upload/ + utils/upload_utils.py)"]
        FU --> FILES["glob data/Avalanche_ORG/*.csv"]
        FILES --> D1{"MAX(file_date) in<br/>geo.avalanche_org_raw?"}
        D1 -->|"table empty"| H1["HISTORICAL: all CSVs"]
        D1 -->|"table has data"| I1["INCREMENTAL: file_date newer than max"]
        H1 --> READ["spark.read.csv with model schema<br/>+ file_date column from filename"]
        I1 --> READ
        READ --> W1["df.write.jdbc append"]
        D1 -->|"no new files"| SKIP1["log warning → return False"]
    end

    subgraph R2P["Step 2 — Raw2Plain (ingestion/raw2plain/ + utils/raw2plain_utils.py)"]
        D2{"MAX(file_date) in<br/>geo.avalanche_org?"}
        D2 -->|"final empty"| H2["HISTORICAL: all raw rows"]
        D2 -->|"final has data"| I2["INCREMENTAL: file_date newer than max"]
        H2 --> CV["convert_fn: cast TEXT to<br/>int / bool / timestamp / date"]
        I2 --> CV
        CV --> W2["write JDBC append"]
        D2 -->|"no new rows"| SKIP2["log warning → return False"]
        FT --> D2
    end

    W1 --> RAW[("geo.avalanche_org_raw<br/>all columns TEXT")]
    RAW --> FT
    W2 --> PLAIN[("geo.avalanche_org<br/>typed columns")]

    SKIP1 -.-> NOOP{"both steps<br/>returned False?"}
    SKIP2 -.-> NOOP
    NOOP -->|"yes"| FAIL["raise RuntimeError →<br/>exit code 1"]
    NOOP -->|"no"| OK["return<br/>'complete'"]
```

How the data flows:

1. `run_etl.py` appends the `_raw` suffix to the table name (unless you already passed one), resolves `JobType.INGESTION` → `jobs/ingestion.py`, and creates a single logger — both steps write to the same `logs/ingestion/ingestion_<table>_<timestamp>.log`.
2. **Upload** — `UploadScriptFactory` picks the upload script for the table; CSVs from `data.directory` are read one by one, stamped with `file_date` from the filename, unioned and appended to the raw table.
3. **Raw2Plain** — `Raw2PlainScriptFactory` picks the transform; only raw rows newer than the latest `file_date` in the plain table are read, passed through the dataset-specific `convert_fn`, and appended to the plain table.
4. Both steps decide [Load type detection](load-types.md) from the state of their target table — no manual flag. A step that finds no new data logs a warning and returns `False`; **when both steps return `False` the job raises `RuntimeError`** (`No new data to ingest for <table>`), so `run_etl.py` exits with code 1 and a chained GEO_E2E run stops. If either step wrote anything the job still finishes successfully — e.g. raw rows left by an earlier crash are transformed even though no new file arrived.

Details of each step: [Upload pipeline](#upload-pipeline) · [Raw2Plain pipeline](#raw2plain-pipeline).

## Upload pipeline

Upload CSV files from GEO_Download to a Supabase table (`ingestion/upload/` scripts are thin wrappers around `utils/upload_utils.py`):

```python
from pyspark.sql import SparkSession
from model.config import AppConfig, SupabaseConnectionConfig
from ingestion.upload.avalanche_org_upload import upload

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

Convert data from `avalanche_org_raw` (all TEXT) to `avalanche_org` (typed: int, bool, datetime, date) (`ingestion/raw2plain/` scripts are thin wrappers around `utils/raw2plain_utils.py`):

```python
from pyspark.sql import SparkSession
from model.config import AppConfig, SupabaseConnectionConfig
from ingestion.raw2plain.avalanche_org_raw2plain import raw2plain

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
