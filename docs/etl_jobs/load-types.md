← [ETL jobs](../etl-jobs.md)

# Load type detection

The INGESTION pipelines, the DATA_QUALITY job, the DATA_SOURCE_FILE job and the SINGLE_ETL scripts decide their load type from the state of their target table — no manual flag is needed (`SINGLE_ETL` and `DATA_SOURCE_FILE` also accept `--load-type` to force one). The VIEW job decides nothing: views are always `HISTORICAL` ([below](#view)).

## INGESTION (CSV → plain table)

| Condition | Load Type | Behavior |
|-----------|-----------|----------|
| Plain table is empty | `HISTORICAL` | Reads and converts all CSV files |
| Plain table has data | `INCREMENTAL` | Reads only CSVs with `file_date` newer than the latest in the table |
| No file newer than the table's max | `INCREMENTAL` | Warns, writes nothing, returns `False` |

## DATA_QUALITY

| Condition | Load Type | Behavior |
|-----------|-----------|----------|
| No metrics stored for this table | `HISTORICAL` | Measures every `file_date` in the source |
| Metrics stored | `INCREMENTAL` | Measures only `file_date` > the stored maximum, and purges metrics whose `file_date` left the source |

Either load type then refreshes the whole-table `overall` snapshot; `--overall` skips load type detection entirely and only writes the `overall` rows. Details: [DATA_QUALITY job — load type notes](data-quality.md#load-type-detection).

## DATA_SOURCE_FILE

| Condition | Load Type | Behavior |
|-----------|-----------|----------|
| No rows stored for this source | `HISTORICAL` | Replaces the source's file rows with every current CSV |
| Rows stored | `INCREMENTAL` | Writes new/changed files, deletes files that vanished, refreshes the source row |
| No new/changed/removed files | `INCREMENTAL` | Refreshes the source row only |
| File dated today or later | — | Skipped; only dates strictly before the current date are published |
| `--load-type historical` | `HISTORICAL` | Full replace even when rows exist |
| `--load-type incremental` | `INCREMENTAL` | Same as auto; an empty table makes every current file new |

A changed file is one whose `created_at`, `snapshot_date`, `records` or `size_kb` differs from the stored row, so re-downloaded/backfilled files are picked up as well as new dates. Details: [DATA_SOURCE_FILE job](data-source-file.md).

## SINGLE_ETL

| Condition | Load Type | Behavior |
|-----------|-----------|----------|
| Target table empty or missing | `HISTORICAL` | Processes every source row |
| Target table has data | `INCREMENTAL` | Processes only rows with `file_date` > the target's maximum |
| No row newer than the target's max | `INCREMENTAL` | Runner returns `loaded=False` → the job warns and raises `RuntimeError` |
| Source has rows but `_convert` yields none | `HISTORICAL`/`INCREMENTAL` | Runner returns `loaded=False` → the job warns and raises `RuntimeError` |
| `--load-type historical` | `HISTORICAL` | Full reprocess even when the target has data |
| `--load-type incremental` | `INCREMENTAL` | Same as auto; falls back to `HISTORICAL` when the target is empty |

The [`union_etl` script](single-etl/union-etl.md) follows the same table: its target is `geo.union_etl`, and an `INCREMENTAL` run filters *each* of the nine source `*_etl` tables with the target's `file_date` maximum before stacking them.

**SINGLE_ETL fails when it loads no records** — the same contract as INGESTION: the shared runner reports the empty run as `EtlOutcome(loaded=False, …)` and `jobs/single_etl.py` warns with that summary and raises, so `run_etl.py` exits with code 1 (the E2E chain then logs a warning and stops). The runner itself never raises — it covers an up-to-date incremental run, an empty source and a conversion that yields zero rows:

```
WARNING No records loaded into avalanche_org_etl — no rows to process in avalanche_org
ERROR ETL job failed   (RuntimeError)   → exit code 1
```

**INGESTION fails when there is nothing to ingest** — the upload warns and the job raises, so `run_etl.py` exits with code 1 (the E2E chain then logs a warning and skips `DATA_QUALITY`):

```
WARNING No new files to upload for avalanche_org — nothing to do, table is up to date
WARNING No new data to ingest for avalanche_org — no file newer than the latest file_date
ERROR ETL job failed   (RuntimeError)   → exit code 1
```

**DATA_QUALITY** keeps the opposite behaviour — with no new `file_date` to measure it warns and still finishes successfully, so an unchanged chain never fails on the quality step:

```
logger.warning("No new file_date to measure for avalanche_org — metrics are up to date")
```

## VIEW

| Condition | Load Type | Behavior |
|-----------|-----------|----------|
| Always | `HISTORICAL` | Re-runs the script's query over the `*_etl` source and replaces the whole `*_vw` view definition |
| Query returns no rows | `HISTORICAL` | Runner returns `loaded=False` → the job warns and raises `RuntimeError`; the existing view stays untouched |

A view has no `file_date` cursor to be incremental from — there is nothing to detect and nothing to force, and `--load-type` is rejected for this job (`--load-type can only be used with the SINGLE_ETL or DATA_SOURCE_FILE job`). Every run proves the query through Spark JDBC and stores it with `CREATE OR REPLACE VIEW`, so the view always reflects the current contents of its source — see [VIEW job](view.md).

**VIEW fails when its query returns no rows** — the same contract as SINGLE_ETL: `utils/view_utils.py` reports the empty run as `EtlOutcome(loaded=False, …)` and `jobs/view.py` warns with that summary and raises, so `run_etl.py` exits with code 1:

```
WARNING No records loaded into union_vw — query returned no rows from union_etl
ERROR ETL job failed   (RuntimeError)   → exit code 1
```

To exercise the INCREMENTAL path, follow the [incremental test workflow](../usage/maintenance.md#incremental-test-workflow).

