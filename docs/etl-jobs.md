← [README](../README.md)

# ETL Jobs

`run_etl.py` is the main CLI entry point. It dispatches jobs through a registry, so adding a new job type means registering a module in `factory/job_factory.py` — no changes to the CLI itself.

## Jobs

- [CREATE TABLE job](etl_jobs/create-table.md) — builds a typed Supabase table from a Pydantic model.
- [INGESTION job](etl_jobs/ingestion.md) — CSV → typed plain-table pipeline, with the upload internals.
- [DATA_QUALITY job](etl_jobs/data-quality.md) — measures a source table and refreshes the whole-table `overall` snapshot; the metrics themselves are documented in [Data Quality metrics](data-quality-metrics.md).
- [DATA_SOURCE_FILE job](etl_jobs/data-source-file.md) — publishes one source's CSV inventory to `geo.data_source_files` with HISTORICAL/INCREMENTAL load detection.
- [SINGLE_ETL job](etl_jobs/single-etl.md) — maps a source table onto the canonical `*_etl` structure (the [`union_etl` script](etl_jobs/single-etl/union-etl.md) stacks them all into one table); per-source column mappings live under [Source scripts](etl_jobs/single-etl.md#source-scripts).
- [VIEW job](etl_jobs/view.md) — defines a `*_vw` view over a `*_etl` table from a query kept in `config.json` (HISTORICAL only).
- [PREDICTION job](etl_jobs/prediction.md) — trains/reuses one PySpark MLlib model per metric group and fills missing dates into `geo.prediction_etl`.
- [Load type detection](etl_jobs/load-types.md) — how every pipeline picks HISTORICAL/INCREMENTAL and behaves when there is nothing to do.

## CLI synopsis

```bash
python run_etl.py <job> <table_name> [--overall] [--load-type {auto,historical,incremental}]
```

| Argument | Required | Valid for | Description |
|----------|----------|-----------|-------------|
| `job` | yes | every job | Positional. One of `CREATE`, `INGESTION`, `DATA_QUALITY`, `DATA_SOURCE_FILE`, `SINGLE_ETL`, `VIEW`, `PREDICTION` — case-insensitive (`create` is accepted, the value is upper-cased before lookup). See [Jobs](#jobs). |
| `table_name` | yes | every job | Positional. For [`CREATE`](etl_jobs/create-table.md) it is the **exact** table to create (`avalanche_org` / `avalanche_org_etl` — one table per run). For [`INGESTION`](etl_jobs/ingestion.md) it is the plain table name, and the job writes directly to it. [`DATA_QUALITY`](etl_jobs/data-quality.md) takes the plain table name (`DATA_QUALITY` adds nothing). [`DATA_SOURCE_FILE`](etl_jobs/data-source-file.md) takes the source folder name (e.g. `EONET`, `USGS_Earthquake`). [`SINGLE_ETL`](etl_jobs/single-etl.md) takes the script name from `single_etl/` (e.g. `avalanche_org_etl`, or `union_etl` to stack every `*_etl` table). [`VIEW`](etl_jobs/view.md) takes the script name from `views/` (e.g. `metrics_avalanche_vw`). [`PREDICTION`](etl_jobs/prediction.md) takes the configured prediction table name (`prediction_etl`). |
| `--overall` | no | `DATA_QUALITY` only | Measure **only** the whole table as one snapshot stored under `file_date = 'overall'` (a normal run already refreshes the aggregate alongside the dated metrics) — see [Overall mode](etl_jobs/data-quality.md#overall-mode). Passing it with any other job aborts with `parser.error` (`--overall can only be used with the DATA_QUALITY job`). |
| `--load-type {auto,historical,incremental}` | no (default `auto`) | `SINGLE_ETL`, `DATA_SOURCE_FILE` | Force the load type instead of auto-detecting it — see [Load type detection](etl_jobs/load-types.md). Forcing `historical` or `incremental` on any other job — including `VIEW`, which is HISTORICAL-only — aborts with `parser.error` (`--load-type can only be used with the SINGLE_ETL or DATA_SOURCE_FILE job`). |

```bash
python run_etl.py create <table_name>              # job name is case-insensitive
python run_etl.py INGESTION <table_name>           # CSV → typed plain table
python run_etl.py DATA_QUALITY <table_name> --overall
python run_etl.py DATA_SOURCE_FILE EONET --load-type historical
python run_etl.py SINGLE_ETL <table_name>_etl --load-type historical
python run_etl.py VIEW <script_name>
python run_etl.py PREDICTION prediction_etl
python run_etl.py DATA_QUALITY <table_name> --load-type historical   # → parser.error
```

## Job registry and dispatch

`JobFactory` (`factory/job_factory.py`) maps a job type from the `JobType` enum (`model/enums.py`) to its module in `jobs/`:

| Job Type | Module | What it does |
|----------|--------|--------------|
| `CREATE` | `jobs/create_table.py` | Creates the schema and one table from its Pydantic DDL |
| `INGESTION` | `jobs/ingestion.py` | Converts CSVs and writes typed rows straight to the plain table |
| `DATA_QUALITY` | `jobs/data_quality.py` | Computes per-`file_date` quality metrics into `geo.data_quality`, and refreshes the whole-table `overall` snapshot |
| `DATA_SOURCE_FILE` | `jobs/data_source_file.py` | Publishes one source's CSV inventory to `geo.data_source_files` (HISTORICAL/INCREMENTAL) |
| `SINGLE_ETL` | `jobs/single_etl.py` | Runs one standalone PySpark script from `single_etl/` (incl. `union_etl`, which stacks every `*_etl` table) |
| `VIEW` | `jobs/view.py` | Runs one standalone PySpark script from `views/` that defines a `*_vw` view over a `*_etl` table (HISTORICAL only) |
| `PREDICTION` | `jobs/prediction.py` | Trains/reuses PySpark MLlib models over the observed daily metrics and rebuilds `geo.prediction_etl` with predicted values for missing dates |

Every run:

1. Reads `config.json` (Spark master, DB schema, JDBC driver) and the `SUPABASE_DB_*` environment variables / `.env` (Supabase credentials).
2. Creates a Spark session connected to the cluster and registers the stage memory logger (see [Logging](logging/stage-memory.md)).
3. Resolves the job type to its module and executes it, passing the table name, config objects and logger.
4. Stops the Spark session.

```bash
python run_etl.py CREATE <table_name>
python run_etl.py INGESTION <table_name>
python run_etl.py DATA_QUALITY <table_name>
python run_etl.py DATA_SOURCE_FILE <source_name>
python run_etl.py SINGLE_ETL <table_name>_etl
python run_etl.py VIEW <script_name>
python run_etl.py PREDICTION prediction_etl
```

