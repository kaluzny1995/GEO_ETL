← [README](../README.md)

# Usage

## Setup

### 1. Create the Conda environment

```bash
conda env create -f environment.yml
conda activate geo_etl_311
```

### 2. Start the Spark cluster

The cluster is managed by Docker Compose in [GEO_E2E](../../GEO_E2E). Make sure the `.env` file there is configured with the correct `GEO_DATA_DIR` path, then:

```bash
cd ../GEO_E2E
docker compose up -d geo_spark_master geo_spark_worker_1 geo_spark_worker_2
```

See [Spark cluster](spark-cluster.md) for ports, Web UIs and the custom image.

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

## Create tables and run ingestion

The `run_etl.py` CLI creates tables in Supabase by generating DDL from Pydantic models:

```bash
conda activate geo_etl_311

# Create raw + plain tables for a data source
python run_etl.py CREATE avalanche_org

# Run full ingestion pipeline (upload CSVs → raw table → typed plain table)
python run_etl.py INGESTION avalanche_org
```

**All registered tables:**

```bash
# Create tables
python run_etl.py CREATE avalanche_org
python run_etl.py CREATE eonet_event
python run_etl.py CREATE gdacs_event
python run_etl.py CREATE ibtracs_cyclone
python run_etl.py CREATE nifc_fire
python run_etl.py CREATE openmeteo_flood
python run_etl.py CREATE usgs_earthquake
python run_etl.py CREATE usgs_nwis
python run_etl.py CREATE usgs_volcano

# Ingest data (upload + transform)
python run_etl.py INGESTION avalanche_org
python run_etl.py INGESTION eonet_event
python run_etl.py INGESTION gdacs_event
python run_etl.py INGESTION ibtracs_cyclone
python run_etl.py INGESTION nifc_fire
python run_etl.py INGESTION openmeteo_flood
python run_etl.py INGESTION usgs_earthquake
python run_etl.py INGESTION usgs_nwis
python run_etl.py INGESTION usgs_volcano
```

> **Note:** The `_raw` suffix is added automatically. Pass `avalanche_org`, not `avalanche_org_raw`.

**How it works:**

1. Reads config from `config.json` (Spark master, DB schema, JDBC driver) and `supabase_config.json` (credentials)
2. Connects to the Spark cluster
3. For `CREATE`: creates the schema if needed, generates DDL from the Pydantic model, executes it against Supabase
4. For `INGESTION`: runs the upload script (CSVs → raw table) then the raw2plain script (raw → typed table) via factories

See [ETL jobs](etl-jobs.md) for the job registry, the CREATE and INGESTION pipelines and their load-type logic.

## Verify Supabase connectivity

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
|geo               |18    |
|information_schema|69    |
|public            |2     |
|realtime          |3     |
|reddit            |9     |
|storage           |8     |
|vault             |2     |
+------------------+------+

Done.
```

## Drop a table

```bash
conda activate geo_etl_311
python run_drop_table.py <table_name>
```

Expected output:

```
Table geo.<table_name> dropped.
```

## Drop a schema

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

## Delete latest file_date data

Delete all rows with the most recent `file_date` from both raw and plain tables. Useful for re-running INCREMENTAL tests:

```bash
conda activate geo_etl_311

# Delete latest file_date for a single data source
python run_delete_last_filedate_data.py avalanche_org

# Delete latest file_date for all data sources
for t in avalanche_org eonet_event gdacs_event ibtracs_cyclone nifc_fire openmeteo_flood usgs_earthquake usgs_nwis usgs_volcano; do
    python run_delete_last_filedate_data.py "$t"
done
```

> **Note:** The `_raw` suffix is added automatically. Pass `avalanche_org`, not `avalanche_org_raw`.

The script derives the plain table name automatically by stripping the `_raw` suffix (e.g. `avalanche_org_raw` → `avalanche_org`).

Expected output:

```
Latest file_date: 2026-09-22
Deleted 166 rows from geo.avalanche_org
Deleted 166 rows from geo.avalanche_org_raw
Delete complete: file_date=2026-09-22, raw=166, plain=166
```

## Incremental test workflow

1. Run HISTORICAL ingestion to load all data (`python run_etl.py INGESTION <table>`).
2. Delete latest `file_date` from both raw + plain tables using `run_delete_last_filedate_data.py`.
3. Run INGESTION again — the script detects missing files and re-loads the deleted data.

Load types and the detection logic are described in [ETL jobs — Load type detection](etl-jobs.md#load-type-detection).

## Standalone pipelines

`upload()` and `raw2plain()` can also be called directly instead of through `run_etl.py` — see [ETL jobs](etl-jobs.md#upload-pipeline) for examples and behavior.
