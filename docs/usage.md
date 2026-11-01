← [README](../README.md)

# Usage

## Setup

### 1. Create the Conda environment

```bash
conda env create -f environment.yml
conda activate geo_etl_311
```

### 2. Configure Supabase credentials

Do this in **GEO_E2E** — its bootstrap script writes this project's `.env` (the same five `SUPABASE_DB_*` values are used there for Airflow's metadata DB), so the credentials are configured once for the whole pipeline:

```bash
cd ../GEO_E2E
./setup.sh          # re-uses an existing .env (--force regenerates it) and mirrors it
cd ../GEO_ETL       # the script mirrored SUPABASE_DB_* into ./.env
```

Manual fallback (standalone or CI): copy the template and fill in the values from the Supabase dashboard (**Connect → Connection pooling → Transaction pooler**; the password is set at project creation or via **Generate password** in Project Settings → Database — full walkthrough in [Configuration — Supabase Credentials](configuration.md#supabase-credentials)):

```bash
cp .env.example .env
# edit .env: SUPABASE_DB_USER, SUPABASE_DB_PASSWORD, SUPABASE_DB_HOST, SUPABASE_DB_PORT, SUPABASE_DB_NAME
```

The `.env` file is gitignored. In CI, export the same variables instead — no file is needed.

### 3. Start the Spark cluster

The cluster is managed by Docker Compose in [GEO_E2E](../../GEO_E2E). Make sure the `.env` file there is configured with the correct `GEO_DATA_DIR` path, then:

```bash
cd ../GEO_E2E
docker compose up -d geo_spark_master geo_spark_worker_1 geo_spark_worker_2
```

See [Spark cluster](spark-cluster.md) for ports, Web UIs and the custom image.

### 4. Verify cluster connectivity

```bash
conda activate geo_etl_311
python run_pyspark_csv_test.py
```

Expected content of `logs/pyspark_csv_test/pyspark_csv_test_<timestamp>.log` (`setup_logger` writes to the file and mirrors to stdout, so the same lines appear on the terminal too):

```
Connected to: spark://localhost:7078
Spark version: 3.5.5
Data path:     /path/to/GEO_Download/data

  Avalanche_ORG                  -> 33 CSV file(s)
  EONET                          -> 32 CSV file(s)
  GDACS                          -> 33 CSV file(s)
  ...
```

The CSV counts are illustrative — they grow with every [GEO_Download](../../GEO_Download) run.

## Create tables and run ingestion

The `run_etl.py` CLI creates tables in Supabase by generating DDL from Pydantic models — one `CREATE` run creates exactly one table, so the raw and the plain table need their own run:

```bash
conda activate geo_etl_311

# Create the tables for a data source — one CREATE per table:
# raw (all-TEXT) and plain (typed)
python run_etl.py CREATE <table_name>_raw
python run_etl.py CREATE <table_name>

# Run full ingestion pipeline (upload CSVs → raw table → typed plain table)
python run_etl.py INGESTION <table_name>

# Measure data quality of the plain table (writes geo.data_quality)
python run_etl.py DATA_QUALITY <table_name>

# Run a standalone PySpark script from single_etl/ (load type auto-detected)
python run_etl.py SINGLE_ETL <table_name>_etl

# Same, forcing the load type
python run_etl.py SINGLE_ETL <table_name>_etl --load-type historical
python run_etl.py SINGLE_ETL <table_name>_etl --load-type incremental

# Stack every *_etl table into one union table (the union script, load type auto-detected)
python run_etl.py CREATE <table_name>
python run_etl.py SINGLE_ETL <table_name>

# Same, forcing the load type
python run_etl.py SINGLE_ETL <table_name> --load-type historical
```

GEO_ETL makes no reverse-geocoding API calls: the lookup and the fetching live in GEO_Download, whose downloaders stamp `geocode_country` / `geocode_state` into the daily CSVs at download time — the scripts below only read those columns and apply their own fallbacks.

`avalanche_org_etl` needs no lookup and no network access: the zone coordinates ship with every source row (`latitude`/`longitude`, polygon centroids computed by GEO_Download), see [ETL jobs — avalanche_org_etl](etl_jobs/single-etl.md#avalanche_org_etl).

`eonet_event_etl` takes the country/state from the event description when it names a US state, otherwise from the `geocode_country`/`geocode_state` columns stamped into the CSVs at download time; open-sea events that neither answers get `ZZ`/`NULL`, see [ETL jobs — eonet_event_etl](etl_jobs/single-etl.md#eonet_event_etl).

`nifc_fire_etl` needs no unit lookup: the full jurisdiction name ships with every source row (`unit_name`, resolved by GEO_Download through its `data/nifc_unit_names.csv` lookup when the daily CSVs are written). The source's own `state` wins; perimeters whose `state` is blank use the `geocode_state` column stamped at download time, see [ETL jobs — nifc_fire_etl](etl_jobs/single-etl.md#nifc_fire_etl).

`openmeteo_flood_etl` maps the source's country names to ISO codes in-script (an unknown name falls back to the `geocode_country` column stamped at download time) and reads the subdivision from `geocode_state`, and joins the threshold reference `data/openmeteo_discharge_thresholds.csv` (built by GEO_Download) that feeds `danger_rating` and `other_metrics`, see [ETL jobs — openmeteo_flood_etl](etl_jobs/single-etl.md#openmeteo_flood_etl).

`usgs_earthquake_etl` parses the country from the `place` suffix first; rows the suffix cannot answer use the `geocode_country`/`geocode_state` columns stamped at download time, see [ETL jobs — usgs_earthquake_etl](etl_jobs/single-etl.md#usgs_earthquake_etl).

`usgs_nwis_etl` parses the river and place names out of `site_name`; the subdivision comes from the `geocode_state` column stamped at download time, and joins the flood-category reference `data/usgs_flood_categories.csv` (kept present by GEO_Download's `USGS_NWIS` downloader) that feeds `danger_rating` and `other_metrics`, see [ETL jobs — usgs_nwis_etl](etl_jobs/single-etl.md#usgs_nwis_etl).

`usgs_volcano_etl` passes the notice details the GEO_Download downloader stamped into the CSVs (coordinates, region, status line, full notice type) straight through; the metric is the stamped NCEI/WDS eruption record (`vei` with unit `VEI`, casualties and damage in `other_metrics`), and `state_province` comes from the `geocode_state` column stamped at download time (`ZZ` when empty, e.g. the Ahyi Seamount ocean point), see [ETL jobs — usgs_volcano_etl](etl_jobs/single-etl.md#usgs_volcano_etl).

**Every registered table — one run per table (the names are listed in [Data tables](data-tables.md)):**

```bash
# Create the raw tables (all TEXT) — one CREATE run per table
python run_etl.py CREATE <table_name>_raw

# Create the plain tables (typed) — again one CREATE run per table
python run_etl.py CREATE <table_name>

# Canonical ETL tables (SINGLE_ETL targets) — one CREATE run per table
python run_etl.py CREATE <table_name>_etl

# Union table — stacks every *_etl table into one (the union script)
python run_etl.py CREATE <table_name>

# Ingest data (upload + transform) — one run per data source
python run_etl.py INGESTION <table_name>
```

> **Note:** `CREATE` takes the exact table name — one run creates one table, so raw and plain tables need their own `CREATE`. Only `INGESTION` appends the `_raw` suffix itself: pass `<table_name>`, not `<table_name>_raw`.

**How it works:**

1. Reads config from `config.json` (Spark master, DB schema, JDBC driver) and the `SUPABASE_DB_*` environment variables / `.env` (Supabase credentials)
2. Connects to the Spark cluster
3. For `CREATE`: creates the schema if needed, generates DDL from the Pydantic model for the **exact** table name passed on the command line, executes it against Supabase — one table per run
4. For `INGESTION`: runs the upload script (CSVs → raw table) then the raw2plain script (raw → typed table) via factories — **exits with code 1 when nothing was ingested** (no CSV newer than the raw table's max `file_date` and no raw row newer than the plain table's)
5. For `DATA_QUALITY`: measures only the selected `file_date`s of the plain table and replaces those rows in `geo.data_quality`, then refreshes the whole-table `overall` snapshot (when not run with `--overall`)
6. For `SINGLE_ETL`: resolves the script name (`single_etl/<name>.py`) via `SingleEtlFactory` and runs it with the selected load type — **exits with code 1 when no record was loaded** (no source row newer than the `_etl` table's max `file_date`, an empty source, or a conversion that yields zero rows). The `union_etl` script is one of them: it stacks every `*_etl` table into `geo.union_etl`
7. For `VIEW`: resolves the script name (`views/<name>.py`) via `ViewScriptFactory`, runs its query over the `*_etl` source through Spark and replaces the `*_vw` view definition (HISTORICAL only) — **exits with code 1 when the query returned no rows**

See [ETL jobs](etl-jobs.md) for the job registry, the CREATE, INGESTION, SINGLE_ETL and VIEW pipelines and their load-type logic.

**Exit codes:** `0` when the job wrote data (or `DATA_QUALITY` only had nothing new to measure and warned), `1` on any exception — including the `INGESTION` "no new data", the `SINGLE_ETL` "no records loaded" and the `VIEW` "query returned no rows" cases. A chained [GEO_E2E](../../GEO_E2E/docs/architecture.md) run reacts to a non-zero code by logging a warning and stopping the chain, so `DATA_QUALITY` never runs on a table that did not change.

## Check data quality

```bash
conda activate geo_etl_311

# One table (plain name — no _raw suffix)
python run_etl.py DATA_QUALITY <table_name>

# One table — whole table as a single snapshot (file_date = 'overall')
python run_etl.py DATA_QUALITY <table_name> --overall

# All plain tables — repeat the first command once per table
```

> **Note:** Pass the plain table name — `DATA_QUALITY` does not add the `_raw` suffix, and the metrics are computed on the typed table.

The first run measures every `file_date` (`HISTORICAL`); later runs measure only the `file_date`s added since (`INCREMENTAL`) and drop metrics for `file_date`s that no longer exist in the source. Run it after each INGESTION to keep `geo.data_quality` in step with the data — see [Load type detection](etl_jobs/load-types.md#data_quality). Each normal run also refreshes the whole-table `overall` snapshot, so the aggregate never falls behind the dated rows — see [Overall refresh](etl_jobs/data-quality.md#overall-refresh).

Add `--overall` to measure **only** the entire table as a single snapshot stored under `file_date = 'overall'` — it writes the `overall` rows, never touches the per-`file_date` rows, and skips load type detection and the dated metrics. A normal run already keeps the aggregate in step, so `--overall` is only needed to refresh the snapshot alone — see [Overall mode](etl_jobs/data-quality.md#overall-mode).

Expected log file content (`logs/data_quality/data_quality_avalanche_org_<timestamp>.log` — the job's logger writes to the file and mirrors to stdout):

```
INFO Creating table 'geo.data_quality' if not exists...
INFO No metrics stored for avalanche_org → HISTORICAL: measuring every file_date
INFO Reading geo.avalanche_org...
INFO Computed 657 metrics for 9 file_date(s) in avalanche_org (HISTORICAL)
INFO Replacing stored metrics for avalanche_org...
INFO Writing to geo.data_quality...
INFO Data quality metrics complete: 657 rows written to geo.data_quality
INFO Refreshing overall snapshot for avalanche_org...
INFO Writing to geo.data_quality...
INFO Overall snapshot refreshed: 73 metrics for avalanche_org
```

Results land in `geo.data_quality` (one row per metric and `file_date`). Re-running the job for the same table replaces its previous rows. Metrics, rules and example SQL: see [Data Quality metrics](data-quality-metrics.md).

## Verify Supabase connectivity

```bash
conda activate geo_etl_311
python run_pyspark_supabase_test.py
```

Expected output — the `Connected to:` … `JDBC URL:` header and `Done.` go to the log file (`logs/pyspark_supabase_test/pyspark_supabase_test_<timestamp>.log`) and are mirrored to stdout; the table itself is printed to stdout by `.show()`:

```
Connected to:   spark://localhost:7078
Spark version:  3.5.5
Supabase host:  aws-1-eu-west-1.pooler.supabase.com
JDBC URL:       jdbc:postgresql://aws-1-eu-west-1.pooler.supabase.com:6543/postgres

+------------------+------+
|schema            |tables|
+------------------+------+
|auth              |27    |
|extensions        |2     |
|geo               |29    |
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

Expected log file content (`logs/drop_table/drop_table_<table_name>_<timestamp>.log`):

```
Table geo.<table_name> dropped.
```

## Drop a schema

Only works if the schema contains no tables:

```bash
conda activate geo_etl_311
python run_drop_schema.py <schema_name>
```

Expected log file content (`logs/drop_schema/drop_schema_<schema_name>_<timestamp>.log`, empty schema):

```
Schema '<schema_name>' dropped.
```

If the schema has tables:

```
Schema '<schema_name>' contains tables. Drop tables first.
```

## Delete latest file_date data

Delete all rows with the most recent `file_date`. Useful for re-running INCREMENTAL tests:

```bash
conda activate geo_etl_311

# Delete latest file_date from the raw + plain tables of one data source
# (repeat once per table for all data sources)
python run_delete_last_filedate_data.py <table_name>

# Delete latest file_date from the ETL table only
python run_delete_last_filedate_data.py <table_name>_etl
```

> **Note:** Both forms are accepted — the script appends the `_raw` suffix itself when you pass the plain name, so `<table_name>` and `<table_name>_raw` do the same thing.

The script derives the plain table name automatically by stripping the `_raw` suffix (e.g. `avalanche_org_raw` → `avalanche_org`).

With the `_etl` suffix the script targets the ETL table only: it reads that table's own latest `file_date` and deletes just those rows, leaving raw and plain untouched.

The `data_quality` table is rejected with a `ValueError` (in any suffix form) — its rows are per-table metric snapshots, not daily source data. Delete metric rows with SQL against `geo.data_quality`, or let the [DATA_QUALITY job](etl_jobs/data-quality.md) purge stale dates automatically.

Expected log file content (`logs/delete_last_filedate/delete_last_filedate_<table>_<timestamp>.log`):

```
Latest file_date: 2026-09-22
Deleted 166 rows from geo.avalanche_org
Deleted 166 rows from geo.avalanche_org_raw
Delete complete: file_date=2026-09-22, raw=166, plain=166
```

## Incremental test workflow

1. Run HISTORICAL ingestion to load all data (`python run_etl.py INGESTION <table_name>`).
2. Delete latest `file_date` from both raw + plain tables using `run_delete_last_filedate_data.py`.
3. Run INGESTION again — the script detects missing files and re-loads the deleted data.

The same pattern works for the ETL step: pass `<table_name>_etl` to the script, then re-run `python run_etl.py SINGLE_ETL <table_name>_etl` to re-process the deleted date incrementally.

Load types and the detection logic are described in [ETL jobs — Load type detection](etl_jobs/load-types.md).

## Standalone pipelines

`upload()` and `raw2plain()` can also be called directly instead of through `run_etl.py` — see [Upload pipeline](etl_jobs/ingestion.md#upload-pipeline) for examples and behavior.
