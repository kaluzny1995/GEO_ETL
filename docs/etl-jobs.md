← [README](../README.md)

# ETL Jobs

`run_etl.py` is the main CLI entry point. It dispatches jobs through a registry, so adding a new job type means registering a module in `factory/job_factory.py` — no changes to the CLI itself.

## Jobs

- [CREATE TABLE job](etl_jobs/create-table.md) — builds a typed Supabase table from a Pydantic model.
- [INGESTION job](etl_jobs/ingestion.md) — CSV upload + Raw2Plain pipelines, with the upload/raw2plain internals.
- [DATA_QUALITY job](etl_jobs/data-quality.md) — measures a source table; the metrics themselves are documented in [Data Quality metrics](data-quality-metrics.md).
- [SINGLE_ETL job](etl_jobs/single-etl.md) — maps a source table onto the canonical `*_etl` structure (the [`union_etl` script](etl_jobs/single-etl.md#union_etl) stacks them all into one table).
- [Load type detection](etl_jobs/load-types.md) — how every pipeline picks HISTORICAL/INCREMENTAL and behaves when there is nothing to do.

## CLI synopsis

```bash
python run_etl.py <job> <table_name> [--overall] [--load-type {auto,historical,incremental}]
```

| Argument | Required | Valid for | Description |
|----------|----------|-----------|-------------|
| `job` | yes | every job | Positional. One of `CREATE`, `INGESTION`, `DATA_QUALITY`, `SINGLE_ETL` — case-insensitive (`create` is accepted, the value is upper-cased before lookup). See [Jobs](#jobs). |
| `table_name` | yes | every job | Positional. For [`CREATE`](etl_jobs/create-table.md) it is the **exact** table to create (`avalanche_org_raw` / `avalanche_org` / `avalanche_org_etl` — one table per run). For [`INGESTION`](etl_jobs/ingestion.md) GEO_ETL appends `_raw` itself, so pass `avalanche_org`. [`DATA_QUALITY`](etl_jobs/data-quality.md) takes the plain table name (`DATA_QUALITY` adds nothing). [`SINGLE_ETL`](etl_jobs/single-etl.md) takes the script name from `single_etl/` (e.g. `avalanche_org_etl`, or `union_etl` to stack every `*_etl` table). |
| `--overall` | no | `DATA_QUALITY` only | Measure the whole table as one snapshot stored under `file_date = 'overall'` — see [Overall mode](etl_jobs/data-quality.md#overall-mode). Passing it with any other job aborts with `parser.error` (`--overall can only be used with the DATA_QUALITY job`). |
| `--load-type {auto,historical,incremental}` | no (default `auto`) | `SINGLE_ETL` only | Force the load type instead of auto-detecting it — see [Load type detection](etl_jobs/load-types.md#single_etl). Forcing `historical` or `incremental` on any other job aborts with `parser.error` (`--load-type can only be used with the SINGLE_ETL job`). |

```bash
python run_etl.py create <table_name>_raw          # job name is case-insensitive
python run_etl.py INGESTION <table_name>           # _raw appended for you
python run_etl.py DATA_QUALITY <table_name> --overall
python run_etl.py SINGLE_ETL <table_name>_etl --load-type historical
python run_etl.py DATA_QUALITY <table_name> --load-type historical   # → parser.error
```

## Job registry and dispatch

`JobFactory` (`factory/job_factory.py`) maps a job type from the `JobType` enum (`model/enums.py`) to its module in `jobs/`:

| Job Type | Module | What it does |
|----------|--------|--------------|
| `CREATE` | `jobs/create_table.py` | Creates the schema and one table from its Pydantic DDL |
| `INGESTION` | `jobs/ingestion.py` | Upload CSVs → raw table, then raw → typed table |
| `DATA_QUALITY` | `jobs/data_quality.py` | Computes per-`file_date` quality metrics into `geo.data_quality` |
| `SINGLE_ETL` | `jobs/single_etl.py` | Runs one standalone PySpark script from `single_etl/` (incl. `union_etl`, which stacks every `*_etl` table) |

Every run:

1. Reads `config.json` (Spark master, DB schema, JDBC driver) and `supabase_config.json` (credentials).
2. Creates a Spark session connected to the cluster and registers the stage memory logger (see [Logging](logging.md#stage-memory-monitoring)).
3. Resolves the job type to its module and executes it, passing the table name, config objects and logger.
4. Stops the Spark session.

```bash
python run_etl.py CREATE <table_name>_raw
python run_etl.py CREATE <table_name>
python run_etl.py INGESTION <table_name>
python run_etl.py DATA_QUALITY <table_name>
python run_etl.py SINGLE_ETL <table_name>_etl
```

