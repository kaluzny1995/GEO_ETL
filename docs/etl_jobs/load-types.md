← [ETL jobs](../etl-jobs.md)

# Load type detection

The INGESTION pipelines, the DATA_QUALITY job and the SINGLE_ETL scripts decide their load type from the state of their target table — no manual flag is needed (`SINGLE_ETL` also accepts `--load-type` to force one).

## Upload

| Condition | Load Type | Behavior |
|-----------|-----------|----------|
| Table is empty | `HISTORICAL` | Uploads all CSV files |
| Table has data | `INCREMENTAL` | Uploads only CSVs with `file_date` newer than the latest in the table |
| No file newer than the table's max | `INCREMENTAL` | Warns, writes nothing, returns `False` |

## Raw2Plain

| Condition | Load Type | Behavior |
|-----------|-----------|----------|
| Final table is empty | `HISTORICAL` | Processes all raw data |
| Final table has data | `INCREMENTAL` | Processes only raw rows with `file_date` > max in final |
| No raw row newer than the final max | `INCREMENTAL` | Warns, writes nothing, returns `False` |

## DATA_QUALITY

| Condition | Load Type | Behavior |
|-----------|-----------|----------|
| No metrics stored for this table | `HISTORICAL` | Measures every `file_date` in the source |
| Metrics stored | `INCREMENTAL` | Measures only `file_date` > the stored maximum, and purges metrics whose `file_date` left the source |

Either load type then refreshes the whole-table `overall` snapshot; `--overall` skips load type detection entirely and only writes the `overall` rows. Details: [DATA_QUALITY job — load type notes](data-quality.md#load-type-detection).

## SINGLE_ETL

| Condition | Load Type | Behavior |
|-----------|-----------|----------|
| Target table empty or missing | `HISTORICAL` | Processes every source row |
| Target table has data | `INCREMENTAL` | Processes only rows with `file_date` > the target's maximum |
| No row newer than the target's max | `INCREMENTAL` | Runner returns `loaded=False` → the job warns and raises `RuntimeError` |
| Source has rows but `_convert` yields none | `HISTORICAL`/`INCREMENTAL` | Runner returns `loaded=False` → the job warns and raises `RuntimeError` |
| `--load-type historical` | `HISTORICAL` | Full reprocess even when the target has data |
| `--load-type incremental` | `INCREMENTAL` | Same as auto; falls back to `HISTORICAL` when the target is empty |

The [`union_etl` script](single-etl.md#union_etl) follows the same table: its target is `geo.union_etl`, and an `INCREMENTAL` run filters *each* of the nine source `*_etl` tables with the target's `file_date` maximum before stacking them.

**SINGLE_ETL fails when it loads no records** — the same contract as INGESTION: the shared runner reports the empty run as `EtlOutcome(loaded=False, …)` and `jobs/single_etl.py` warns with that summary and raises, so `run_etl.py` exits with code 1 (the E2E chain then logs a warning and stops). The runner itself never raises — it covers an up-to-date incremental run, an empty source and a conversion that yields zero rows:

```
WARNING No records loaded into avalanche_org_etl — no rows to process in avalanche_org
ERROR ETL job failed   (RuntimeError)   → exit code 1
```

**INGESTION fails when there is nothing to ingest** — both steps warn, and because neither wrote anything the job raises, so `run_etl.py` exits with code 1 (the E2E chain then logs a warning and skips `DATA_QUALITY`):

```
WARNING No new files to upload for avalanche_org_raw — nothing to do, table is up to date
WARNING No new records to process for avalanche_org — nothing to do, table is up to date
WARNING No new data to ingest for avalanche_org_raw — no file newer than the latest file_date
ERROR ETL job failed   (RuntimeError)   → exit code 1
```

A step that *did* have work keeps the job green: raw rows left behind by an earlier crash are transformed even when no new file arrived.

**DATA_QUALITY** keeps the opposite behaviour — with no new `file_date` to measure it warns and still finishes successfully, so an unchanged chain never fails on the quality step:

```
logger.warning("No new file_date to measure for avalanche_org — metrics are up to date")
```

To exercise the INCREMENTAL path, follow the [incremental test workflow](../usage.md#incremental-test-workflow).

