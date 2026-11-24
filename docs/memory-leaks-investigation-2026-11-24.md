# Memory leaks investigation 2026-11-24

## Memory leaks investigation 2026-11-24

### Symptom

Host memory was **33 % before the 2026-11-24 pipeline run and 35.2 % after it**
(+~1.4 GiB on a 62.5 GiB host). This follow-up checks whether the daily
PySpark/MLlib run left memory behind.

### Finding

No ETL leak. Each job is still a short-lived `run_etl.py` subprocess with its
own PySpark driver; after the 07:00–07:22 run there was no live `conda run`,
`run_etl` or Spark driver process in the scheduler container.

| Measurement after the run | Value |
|---------------------------|------:|
| Scheduler cgroup current | 0.58 GiB (peak 2.45 GiB during ETL) |
| Spark worker 1 | 255.3 MiB / 3 GiB |
| Spark worker 2 | 267.9 MiB / 3 GiB |
| All nine GEO containers | 1.95 GiB |
| `gnome-system-monitor` | 10.65 GiB, +0.87 GiB since 23 Nov |
| Chrome | 6.54 GiB, +0.21 GiB since 23 Nov |

The worker cgroups returned to baseline after the ETL peak, and the scheduler
task log shows host memory at 21.43 GiB (07:00) → 21.57 GiB (07:22), inside
normal fluctuation.

### Code status

No new leak and no code change today. Yesterday's changes are deployed:
`_merge_logs_by_timestamp` streams the Python/Spark/memory logs instead of
materializing them, and PREDICTION persists its stacked frames with
`MEMORY_AND_DISK` and unpersists them in `finally`. No DataFrame, RDD or
broadcast survives a driver process.

### Verification

```bash
cd GEO_ETL
conda run -n geo_etl_311 python -m pytest -q
docker stats --no-stream
```

### Conclusion

GEO_ETL does not explain the host increase; the transient Spark/executor peak
is expected and is released. Closing or restarting `gnome-system-monitor`
releases its ~10.6 GiB history buffer. After the monitor exited at ~18:44, host used fell from 22.0 GiB (35.2 %) to 11.1 GiB (17.6 %), confirming the attribution. Full evidence is in
[GEO_E2E — Memory leaks investigation 2026-11-24](../../GEO_E2E/docs/memory-leaks-investigation-2026-11-24.md).
