← [Usage](../usage.md)

# Maintenance

Connectivity checks, dropping tables and schemas, and resetting the latest `file_date`
for incremental tests.

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
|geo               |39    |
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

# Delete latest file_date from a plain table
# (repeat once per table for all data sources)
python run_delete_last_filedate_data.py <table_name>

# Delete latest file_date from the ETL table only
python run_delete_last_filedate_data.py <table_name>_etl
```

With the `_etl` suffix the script targets the ETL table only: it reads that table's own latest `file_date` and deletes just those rows. Without it, the script targets the plain table named on the command line.

The `data_quality` table is rejected with a `ValueError` — its rows are per-table metric snapshots, not daily source data. Delete metric rows with SQL against `geo.data_quality`, or let the [DATA_QUALITY job](../etl_jobs/data-quality.md) purge stale dates automatically.

Expected log file content (`logs/delete_last_filedate/delete_last_filedate_<table>_<timestamp>.log`):

```
Latest file_date: 2026-09-22
Deleted 166 rows from geo.avalanche_org
Delete complete: file_date=2026-09-22, table=avalanche_org, rows=166
```

## Incremental test workflow

1. Run HISTORICAL ingestion to load all data (`python run_etl.py INGESTION <table_name>`).
2. Delete latest `file_date` from the plain table using `run_delete_last_filedate_data.py`.
3. Run INGESTION again — the script detects missing files and re-loads the deleted data.

The same pattern works for the ETL step: pass `<table_name>_etl` to the script, then re-run `python run_etl.py SINGLE_ETL <table_name>_etl` to re-process the deleted date incrementally.

Load types and the detection logic are described in [ETL jobs — Load type detection](../etl_jobs/load-types.md).
