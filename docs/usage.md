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

# Measure data quality of the plain table (writes geo.data_quality)
python run_etl.py DATA_QUALITY avalanche_org

# Run a standalone PySpark script from single_etl/ (load type auto-detected)
python run_etl.py SINGLE_ETL avalanche_org_etl

# Same, forcing the load type
python run_etl.py SINGLE_ETL avalanche_org_etl --load-type historical
python run_etl.py SINGLE_ETL avalanche_org_etl --load-type incremental
```

`avalanche_org_etl` reads the zone centroid lookup (`cache/avalanche_zone_centroids.csv`), which it builds automatically on the first run — refresh it with `python -m utils.fetch_avalanche_zones`, see [ETL jobs — avalanche_org_etl](etl_jobs/single-etl.md#avalanche_org_etl).

`eonet_event_etl` reverse-geocodes the coordinates it has not seen before into `cache/reverse_geocode.csv` (built automatically, one entry per new coordinate), see [ETL jobs — eonet_event_etl](etl_jobs/single-etl.md#eonet_event_etl).

`nifc_fire_etl` resolves jurisdiction unit codes into `cache/nifc_unit_names.csv` (built automatically on the first run, refreshed when the data carries an unknown code) and reverse-geocodes the perimeters that predate the `state` field into the shared `cache/reverse_geocode.csv`, see [ETL jobs — nifc_fire_etl](etl_jobs/single-etl.md#nifc_fire_etl).

`openmeteo_flood_etl` maps the source's country names to ISO codes in-script and reverse-geocodes every river reference point once into the shared `cache/reverse_geocode.csv` (~111 coordinates), see [ETL jobs — openmeteo_flood_etl](etl_jobs/single-etl.md#openmeteo_flood_etl).

`usgs_earthquake_etl` parses the country from the `place` suffix and reverse-geocodes only the ~93 remote places that have no usable suffix into the shared `cache/reverse_geocode.csv`, see [ETL jobs — usgs_earthquake_etl](etl_jobs/single-etl.md#usgs_earthquake_etl).

`usgs_nwis_etl` parses the river and place names out of `site_name` and reverse-geocodes the gauge point of every site once into the shared `cache/reverse_geocode.csv` (1 coordinate today), see [ETL jobs — usgs_nwis_etl](etl_jobs/single-etl.md#usgs_nwis_etl).

`usgs_volcano_etl` fetches the coordinates, region, status line and full notice type of every notice once into `cache/usgs_volcano_notices.csv` (built automatically on the first run, immutable notices are never refetched) and reverse-geocodes the volcano points into the shared `cache/reverse_geocode.csv` (4 coordinates today), see [ETL jobs — usgs_volcano_etl](etl_jobs/single-etl.md#usgs_volcano_etl).

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

# Canonical ETL table (SINGLE_ETL target)
python run_etl.py CREATE avalanche_org_etl
python run_etl.py CREATE eonet_event_etl
python run_etl.py CREATE gdacs_event_etl
python run_etl.py CREATE ibtracs_cyclone_etl
python run_etl.py CREATE nifc_fire_etl
python run_etl.py CREATE openmeteo_flood_etl
python run_etl.py CREATE usgs_earthquake_etl
python run_etl.py CREATE usgs_nwis_etl
python run_etl.py CREATE usgs_volcano_etl

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
4. For `INGESTION`: runs the upload script (CSVs → raw table) then the raw2plain script (raw → typed table) via factories — **exits with code 1 when nothing was ingested** (no CSV newer than the raw table's max `file_date` and no raw row newer than the plain table's)
5. For `DATA_QUALITY`: measures only the selected `file_date`s of the plain table and replaces those rows in `geo.data_quality`
6. For `SINGLE_ETL`: resolves the script name (`single_etl/<name>.py`) via `SingleEtlFactory` and runs it with the selected load type

See [ETL jobs](etl-jobs.md) for the job registry, the CREATE, INGESTION and SINGLE_ETL pipelines and their load-type logic.

**Exit codes:** `0` when the job wrote data (or `DATA_QUALITY` only had nothing new to measure and warned), `1` on any exception — including the `INGESTION` "no new data" case. A chained [GEO_E2E](../../GEO_E2E/docs/architecture.md) run reacts to a non-zero code by logging a warning and stopping the chain, so `DATA_QUALITY` never runs on a table that did not change.

## Check data quality

```bash
conda activate geo_etl_311

# One table
python run_etl.py DATA_QUALITY avalanche_org

# One table — whole table as a single snapshot (file_date = 'overall')
python run_etl.py DATA_QUALITY avalanche_org --overall

# All plain tables
for t in avalanche_org eonet_event gdacs_event ibtracs_cyclone nifc_fire openmeteo_flood usgs_earthquake usgs_nwis usgs_volcano; do
    python run_etl.py DATA_QUALITY "$t"
done
```

> **Note:** Pass the plain table name — `DATA_QUALITY` does not add the `_raw` suffix, and the metrics are computed on the typed table.

The first run measures every `file_date` (`HISTORICAL`); later runs measure only the `file_date`s added since (`INCREMENTAL`) and drop metrics for `file_date`s that no longer exist in the source. Run it after each INGESTION to keep `geo.data_quality` in step with the data — see [Load type detection](etl_jobs/load-types.md#data_quality).

Add `--overall` to measure the entire table as a single snapshot stored under `file_date = 'overall'` instead — it replaces a previously stored snapshot on every run and never touches the per-`file_date` rows — see [Overall mode](etl_jobs/data-quality.md#overall-mode).

Expected output:

```
INFO Creating table 'geo.data_quality' if not exists...
INFO No metrics stored for avalanche_org → HISTORICAL: measuring every file_date
INFO Reading geo.avalanche_org...
INFO Computed 657 metrics for 9 file_date(s) in avalanche_org (HISTORICAL)
INFO Replacing stored metrics for avalanche_org...
INFO Writing to geo.data_quality...
INFO Data quality metrics complete: 657 rows written to geo.data_quality
```

Results land in `geo.data_quality` (one row per metric and `file_date`). Re-running the job for the same table replaces its previous rows. Metrics, rules and example SQL: see [Data Quality metrics](data-quality-metrics.md).

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
|geo               |28    |
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

Load types and the detection logic are described in [ETL jobs — Load type detection](etl_jobs/load-types.md).

## Standalone pipelines

`upload()` and `raw2plain()` can also be called directly instead of through `run_etl.py` — see [Upload pipeline](etl_jobs/ingestion.md#upload-pipeline) for examples and behavior.
