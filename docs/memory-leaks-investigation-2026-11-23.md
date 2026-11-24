# Memory leaks investigation 2026-11-23

## Memory leaks investigation 2026-11-23

### Symptom

The stack had been up since 2026-11-17 without an ETL error, but host memory
recorded by the GEO_E2E task callbacks grew from **11.92 GiB to 19.63 GiB**
(about **1.3 GiB/day**); the user-reported span was 8.7 → 22.2 GiB.

### Root cause

The growth was in the desktop session: `gnome-system-monitor` held **9.74 GiB
RSS** and grew ~**1.1 GiB/day**; Chrome held ~6.3 GiB. The nine GEO containers
stayed at ~2.6–3.4 GiB combined, and the two Spark workers at ~0.3–0.4 GiB each
after their ETL peak. The Spark worker cgroups reached ~2.3–2.4 GiB during a
job and returned to baseline, which is expected transient execution memory, not
a leak.

### GEO_ETL audit

Each ETL job is a `conda run python run_etl.py …` subprocess with its own
PySpark driver. The driver exits after `spark.stop()`, so its heap cannot
survive into the next day. Inside that process:

* `run_etl` persists the converted frame with `MEMORY_AND_DISK` and unpersists
  it in `finally`.
* `data_quality` caches `df_full` / `metrics_df` and releases both in nested
  `finally` blocks.
* `prediction` cached `daily` and released it; MLlib models are persisted to
  disk and loaded one group at a time.
* No module-level DataFrame, RDD or broadcast is kept between jobs.

Two peak-memory issues remained in the PREDICTION job and the log post-processing:

1. **Log merge**: `_merge_logs_by_timestamp` read the whole Python task log
   into a list, appended every Spark stderr line, sorted a second list and kept
   both alive while rewriting the file. A 30-minute job can emit tens of MiB of
   Log4j stderr, so this duplicated a large part of the task log at the end of
   every ETL task.
2. **Prediction lineage**: the job unioned the predictions of every metric
   group into one lazy DataFrame, then called `count()` and `write.jdbc()`.
   Both actions recomputed the full union and kept every intermediate MLlib
   plan alive at once.

### Changes made

* `run_etl._merge_logs_by_timestamp` now merges the three individually
  chronological inputs with `heapq.merge` and writes through a temporary file
  that is atomically moved over the main log. Peak memory is one line per source,
  not two copies of the whole file. The production path also mirrors Spark /
  memory lines to stdout *while merging* (`mirror_to_stdout=True`) instead of
  building a second list of every captured line. The old return-a-list behaviour
  is preserved for callers and tests.
* `jobs/prediction` stacks predictions through `_stack_predictions()` and wraps
  the result in `_persisted()`: `persist(MEMORY_AND_DISK)` once before `count()`
  + `write.jdbc()`, `unpersist()` in `finally` even when the write fails.
  Predictions can now spill to executor disk instead of recomputing ~100 group
  lineages or exhausting the driver heap.

Tests: `test/scripts/test_run_etl.py` covers the merged order and the streaming
stdout path; `test/jobs/test_prediction.py` covers the union helper, the
MEMORY_AND_DISK persistence and the unpersist-on-failure guarantee.

### Verification

```bash
cd GEO_ETL
conda run -n geo_etl_311 python -m pytest -q
# stage memory is still recorded per Spark stage
tail -20 logs/memory/stage_memory_*.log
```

### Conclusion

GEO_ETL did not cause the multi-gigabyte cross-day growth; its Spark drivers are
per-task subprocesses and their caches are released. The two peak-memory fixes
reduce the transient footprint of every ETL task and prevent a large Spark
stderr/PREDICTION lineage from being duplicated in the driver.
