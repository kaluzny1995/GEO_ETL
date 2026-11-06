← [README](../README.md)

# Usage

## Dockerized pipeline

The supported end-to-end run uses the `geo_airflow_spark` image from [GEO_E2E](../../GEO_E2E):

```bash
cd ../GEO_E2E
cp .env.example .env          # fill in the five SUPABASE_DB_* values
./up.sh -d --build            # preflight (six repos + Supabase .env), then compose up
```

The image contains Anaconda, `geo_etl_311`, GEO_ETL under `/opt/geo/GEO_ETL` and the Spark cluster. `SPARK_MASTER_URL=spark://geo-spark-master:7078` is set by Compose; `airflow-init` writes `/opt/geo/GEO_ETL/.env` from the Compose credentials. Rebuild after changing GEO_ETL with `docker compose build airflow-init && ./up.sh -d`.

The host-conda workflow below remains available for development and standalone jobs.

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

Manual fallback (standalone or CI): copy the template and fill in the values from the Supabase dashboard (**Connect → Connection pooling → Transaction pooler**; the password is set at project creation or via **Generate password** in Project Settings → Database — full walkthrough in [Configuration — Supabase Credentials](configuration/supabase-credentials.md)):

```bash
cp .env.example .env
# edit .env: SUPABASE_DB_USER, SUPABASE_DB_PASSWORD, SUPABASE_DB_HOST, SUPABASE_DB_PORT, SUPABASE_DB_NAME
```

The `.env` file is gitignored. In CI, export the same variables instead — no file is needed.

### 3. Start the Spark cluster

The cluster is managed by Docker Compose in [GEO_E2E](../../GEO_E2E). For a host-conda run, build the shared image once and start only the Spark services (the host `GEO_Download/data` folder is bind-mounted by default via `GEO_DATA_DIR` in `GEO_E2E/.env`):

```bash
cd ../GEO_E2E
docker compose build airflow-init   # builds geo_airflow_spark
./up.sh -d geo-spark-master geo-spark-worker-1 geo-spark-worker-2
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

The `run_etl.py` CLI creates tables in Supabase by generating DDL from Pydantic models — one `CREATE` run creates exactly one table:

```bash
conda activate geo_etl_311

# Create the plain table for a data source
python run_etl.py CREATE <table_name>

# Run ingestion (read CSVs, convert to typed columns, write the plain table)
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

`avalanche_org_etl` needs no lookup and no network access: the zone coordinates ship with every source row (`latitude`/`longitude`, polygon centroids computed by GEO_Download), see [ETL jobs — avalanche_org_etl](etl_jobs/single-etl/avalanche-org.md).

`eonet_event_etl` takes the country/state from the event description when it names a US state, otherwise from the `geocode_country`/`geocode_state` columns stamped into the CSVs at download time; open-sea events that neither answers get `ZZ`/`NULL`, see [ETL jobs — eonet_event_etl](etl_jobs/single-etl/eonet-event.md).

`nifc_fire_etl` needs no unit lookup: the full jurisdiction name ships with every source row (`unit_name`, resolved by GEO_Download through its `data/nifc_unit_names.csv` lookup when the daily CSVs are written). The source's own `state` wins; perimeters whose `state` is blank use the `geocode_state` column stamped at download time, see [ETL jobs — nifc_fire_etl](etl_jobs/single-etl/nifc-fire.md).

`openmeteo_flood_etl` maps the source's country names to ISO codes in-script (an unknown name falls back to the `geocode_country` column stamped at download time) and reads the subdivision from `geocode_state`, and joins the threshold reference `data/openmeteo_discharge_thresholds.csv` (built by GEO_Download) that feeds `danger_rating` and `other_metrics`, see [ETL jobs — openmeteo_flood_etl](etl_jobs/single-etl/openmeteo-flood.md).

`usgs_earthquake_etl` parses the country from the `place` suffix first; rows the suffix cannot answer use the `geocode_country`/`geocode_state` columns stamped at download time, see [ETL jobs — usgs_earthquake_etl](etl_jobs/single-etl/usgs-earthquake.md).

`usgs_nwis_etl` parses the river and place names out of `site_name`; the subdivision comes from the `geocode_state` column stamped at download time, and joins the flood-category reference `data/usgs_flood_categories.csv` (kept present by GEO_Download's `USGS_NWIS` downloader) that feeds `danger_rating` and `other_metrics`, see [ETL jobs — usgs_nwis_etl](etl_jobs/single-etl/usgs-nwis.md).

`usgs_volcano_etl` passes the notice details the GEO_Download downloader stamped into the CSVs (coordinates, region, status line, full notice type) straight through; the metric is the stamped NCEI/WDS eruption record (`vei` with unit `VEI`, casualties and damage in `other_metrics`), and `state_province` comes from the `geocode_state` column stamped at download time (`ZZ` when empty, e.g. the Ahyi Seamount ocean point), see [ETL jobs — usgs_volcano_etl](etl_jobs/single-etl/usgs-volcano.md).

**Every registered table — one run per table (the names are listed in [Data tables](data-tables.md)):**

```bash
# Create the plain tables (typed) — one CREATE run per table
python run_etl.py CREATE <table_name>

# Canonical ETL tables (SINGLE_ETL targets) — one CREATE run per table
python run_etl.py CREATE <table_name>_etl

# Union table — stacks every *_etl table into one (the union script)
python run_etl.py CREATE <table_name>

# Ingest data (CSV → typed plain table) — one run per data source
python run_etl.py INGESTION <table_name>
```

> **Note:** `CREATE` takes the exact table name — one run creates one table. `INGESTION` also takes the plain table name and writes directly to it.

**How it works:**

1. Reads config from `config.json` (Spark master, DB schema, JDBC driver) and the `SUPABASE_DB_*` environment variables / `.env` (Supabase credentials)
2. Connects to the Spark cluster
3. For `CREATE`: creates the schema if needed, generates DDL from the Pydantic model for the **exact** table name passed on the command line, executes it against Supabase — one table per run
4. For `INGESTION`: runs the upload script (CSVs → typed plain table) via the factory — **exits with code 1 when nothing was ingested** (no CSV newer than the plain table's max `file_date`)
5. For `DATA_QUALITY`: measures only the selected `file_date`s of the plain table and replaces those rows in `geo.data_quality`, then refreshes the whole-table `overall` snapshot (when not run with `--overall`)
6. For `SINGLE_ETL`: resolves the script name (`single_etl/<name>.py`) via `SingleEtlFactory` and runs it with the selected load type — **exits with code 1 when no record was loaded** (no source row newer than the `_etl` table's max `file_date`, an empty source, or a conversion that yields zero rows). The `union_etl` script is one of them: it stacks every `*_etl` table into `geo.union_etl`
7. For `VIEW`: resolves the script name (`views/<name>.py`) via `ViewScriptFactory`, runs its query over the `*_etl` source through Spark and replaces the `*_vw` view definition (HISTORICAL only) — **exits with code 1 when the query returned no rows**

See [ETL jobs](etl-jobs.md) for the job registry, the CREATE, INGESTION, SINGLE_ETL and VIEW pipelines and their load-type logic.

**Exit codes:** `0` when the job wrote data (or `DATA_QUALITY` only had nothing new to measure and warned, or `DATA_SOURCE_FILE` found no changed files and refreshed only the source row), `1` on any exception — including the `INGESTION` "no new data", the `SINGLE_ETL` "no records loaded" and the `VIEW` "query returned no rows" cases. A chained [GEO_E2E](../../GEO_E2E/docs/architecture.md) run reacts to a non-zero code by logging a warning and stopping the chain, so `DATA_QUALITY` never runs on a table that did not change.

## Data quality and maintenance

- [Check data quality](usage/data-quality.md) — run `DATA_QUALITY`, use `--overall` mode
  and read the expected log output.
- [Maintenance](usage/maintenance.md) — verify Supabase connectivity, drop a table or
  schema, delete the latest `file_date` and re-run incremental tests.

## Standalone pipelines

`upload()` can also be called directly instead of through `run_etl.py` — see [Upload pipeline](etl_jobs/ingestion.md#upload-pipeline) for examples and behavior.
