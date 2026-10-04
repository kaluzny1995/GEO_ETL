← [README](../README.md)

# ETL Jobs

`run_etl.py` is the main CLI entry point. It dispatches jobs through a registry, so adding a new job type means registering a module in `factory/job_factory.py` — no changes to the CLI itself.

## Jobs

- [CREATE TABLE job](etl_jobs/create-table.md) — builds a typed Supabase table from a Pydantic model.
- [INGESTION job](etl_jobs/ingestion.md) — CSV upload + Raw2Plain pipelines, with the upload/raw2plain internals.
- [DATA_QUALITY job](etl_jobs/data-quality.md) — measures a source table; the metrics themselves are documented in [Data Quality metrics](data-quality-metrics.md).
- [SINGLE_ETL job](etl_jobs/single-etl.md) — maps a source table onto the canonical `*_etl` structure.
- [Load type detection](etl_jobs/load-types.md) — how every pipeline picks HISTORICAL/INCREMENTAL and behaves when there is nothing to do.

## Job registry and dispatch

`JobFactory` (`factory/job_factory.py`) maps a job type from the `JobType` enum (`model/enums.py`) to its module in `jobs/`:

| Job Type | Module | What it does |
|----------|--------|--------------|
| `CREATE` | `jobs/create_table.py` | Creates the schema and the tables from Pydantic DDL |
| `INGESTION` | `jobs/ingestion.py` | Upload CSVs → raw table, then raw → typed table |
| `DATA_QUALITY` | `jobs/data_quality.py` | Computes per-`file_date` quality metrics into `geo.data_quality` |
| `SINGLE_ETL` | `jobs/single_etl.py` | Runs one standalone PySpark script from `single_etl/` |

Every run:

1. Reads `config.json` (Spark master, DB schema, JDBC driver) and `supabase_config.json` (credentials).
2. Creates a Spark session connected to the cluster and registers the stage memory logger (see [Logging](logging.md#stage-memory-monitoring)).
3. Resolves the job type to its module and executes it, passing the table name, config objects and logger.
4. Stops the Spark session.

```bash
python run_etl.py CREATE avalanche_org
python run_etl.py INGESTION avalanche_org
python run_etl.py DATA_QUALITY avalanche_org
python run_etl.py SINGLE_ETL avalanche_org_etl
```

