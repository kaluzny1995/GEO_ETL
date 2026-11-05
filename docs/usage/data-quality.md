← [Usage](../usage.md)

# Check data quality

```bash
conda activate geo_etl_311

# One table (plain name)
python run_etl.py DATA_QUALITY <table_name>

# One table — whole table as a single snapshot (file_date = 'overall')
python run_etl.py DATA_QUALITY <table_name> --overall

# All plain tables — repeat the first command once per table
```

> **Note:** Pass the plain table name — the metrics are computed on the typed table.

The first run measures every `file_date` (`HISTORICAL`); later runs measure only the `file_date`s added since (`INCREMENTAL`) and drop metrics for `file_date`s that no longer exist in the source. Run it after each INGESTION to keep `geo.data_quality` in step with the data — see [Load type detection](../etl_jobs/load-types.md#data_quality). Each normal run also refreshes the whole-table `overall` snapshot, so the aggregate never falls behind the dated rows — see [Overall refresh](../etl_jobs/data-quality.md#overall-refresh).

Add `--overall` to measure **only** the entire table as a single snapshot stored under `file_date = 'overall'` — it writes the `overall` rows, never touches the per-`file_date` rows, and skips load type detection and the dated metrics. A normal run already keeps the aggregate in step, so `--overall` is only needed to refresh the snapshot alone — see [Overall mode](../etl_jobs/data-quality.md#overall-mode).

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

Results land in `geo.data_quality` (one row per metric and `file_date`). Re-running the job for the same table replaces its previous rows. Metrics, rules and example SQL: see [Data Quality metrics](../data-quality-metrics.md).
