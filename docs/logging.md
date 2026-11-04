← [README](../README.md)

# Logging

Every script writes structured log files to `logs/`, organized by job type. The logging setup uses the same pattern as [GEO_E2E](../../GEO_E2E) and [GEO_Download](../../GEO_Download).

When called via the INGESTION job, `upload()` and `raw2plain()` receive the ingestion logger, so all logs are consolidated into a single ingestion log file. When called standalone, they create their own separate log files.

**Spark internal logs** (Log4j output from the Spark driver) are captured via a file-descriptor redirect before the JVM starts and merged into the same log file with a `[spark]` prefix when the job completes. This means every `.log` file contains both our application messages and Spark's internal diagnostics in one place.

## Stdout mirroring (orchestrator task logs)

`setup_logger` attaches **two** handlers: the `FileHandler` above **and** a plain `logging.StreamHandler(sys.stdout)` with the same formatter. Every record is therefore written to the log file *and* mirrored to **stdout**. The point is subprocess orchestrators — in [GEO_E2E](../../GEO_E2E) each Airflow task runs `conda run --no-capture-output …`, so stdout is captured into the Airflow task log (web UI → task → **Log**). Without the mirror the UI would show only the wrapper's own lines and an empty `Output:` block, while the full job output sat unread in `logs/`.

The stdout target is deliberately **stdout**, not stderr: `run_etl.py`'s `_StderrCapture` fd-redirects **stderr** (fd 2) to a temp file to capture the JVM's Log4j output — a stderr handler would be swallowed into that temp file instead of reaching the orchestrator.

`run_etl.py` additionally prints the JVM/fd-redirected lines to stdout when the job ends: `_merge_logs_by_timestamp` returns the `[spark]`-prefixed stderr lines plus the stage-memory lines (in timestamp order) and `main()` flushes each to stdout. The Python lines already stream there live via the stdout handler; the JVM lines only exist after the merge, so they are appended at the end. The **file** keeps the fully merged chronological view — the stdout order (live Python, then spark/memory at the end) is a byproduct of when each source becomes available.

## Log structure

```
logs/
├── ingestion/
│   └── ingestion_avalanche_org_2026-10-03T22:31:51.141617.log
├── create/
│   └── create_avalanche_org_raw_2026-10-03T22:31:01.313539.log
├── single_etl/
│   └── single_etl_avalanche_org_etl_2026-10-04T21:09:27.962397.log
├── view/
│   └── view_union_vw_<timestamp>.log
├── data_quality/
│   └── data_quality_avalanche_org_2026-09-27T17:50:36.838342.log
├── drop_table/
│   └── drop_table_avalanche_org_2026-10-03T22:30:44.744051.log
├── drop_schema/
│   └── drop_schema_geo_<timestamp>.log
├── delete_last_filedate/
│   └── delete_last_filedate_avalanche_org_raw_<timestamp>.log
├── memory/
│   └── stage_memory_<timestamp>.log            # transient — merged into the job log, then deleted
├── upload/                          # standalone only
│   └── upload_avalanche_org_raw_<timestamp>.log
├── raw2plain/                       # standalone only
│   └── raw2plain_avalanche_org_<timestamp>.log
├── pyspark_csv_test/
│   └── pyspark_csv_test_<timestamp>.log
└── pyspark_supabase_test/
    └── pyspark_supabase_test_<timestamp>.log
```

Directory names come from the job name (`run_etl.py` lowercases the `JobType` value), so the `CREATE` job writes to `logs/create/` with the file pattern `create_<table>_<timestamp>.log` — one file per run, named after the exact table passed on the command line (`create_avalanche_org_raw_…`, `create_avalanche_org_…`, `create_avalanche_org_etl_…`). An empty `logs/create_table/` directory may exist from an earlier naming scheme; nothing writes to it any more.

## Log format

After a `run_etl.py` job finishes, `_merge_logs_by_timestamp` (`run_etl.py`) rewrites the file so the Python lines, the captured Spark stderr lines and the stage memory lines sit together. A real `logs/create/create_…log` looks like this:

```
[spark] 26/10/04 03:18:38 WARN Utils: Your hostname ... resolves to a loopback address ... ; using 192.168.0.174 instead
[spark] 26/10/04 03:18:38 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
[spark] Setting default log level to "WARN".
[spark] 26/10/04 03:18:38 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
2026-10-04 03:18:37,482 INFO ---- ETL job started: create avalanche_org_raw ----

2026-10-04 03:18:40,267 INFO Connected to Spark: spark://localhost:7078

2026-10-04 03:18:40,268 INFO Spark version: 3.5.5

2026-10-04 03:18:40,268 INFO Creating schema 'geo' if not exists...

2026-10-04 03:18:41,184 INFO Schema 'geo' ready

2026-10-04 03:18:41,185 INFO Generated DDL for geo.avalanche_org_raw

2026-10-04 03:18:41,185 INFO Executing DDL against Supabase...

2026-10-04 03:18:41,596 INFO DDL executed successfully

2026-10-04 03:18:41,596 INFO Table geo.avalanche_org_raw created.

2026-10-04 03:18:42,028 INFO Spark session stopped
```

Python lines carry the usual `%(asctime)s %(levelname)s %(message)s` prefix (`2026-10-04 03:18:41,596 INFO …`) and are separated by a blank line — the merge rewrites every original line together with its newline. `[spark]` lines are raw Log4j output from Spark's stderr with no Python timestamp or level: their Log4j timestamps (`26/10/04 …`) do not match the `YYYY-MM-DD HH:MM:SS` key the merge sorts on, so they sort to the top of the file. Stage memory lines (see below) use `YYYY-MM-DD HH:MM:SS` and interleave with the Python lines in chronological order.

## Logger usage in code

```python
from utils.logger import setup_logger

logger = setup_logger(
    name="my_job",
    log_file=f"logs/my_job/my_job_{dt.datetime.now().isoformat()}.log",
)
logger.info("Job started")
logger.warning("Something looks off")
logger.error("Something failed")
logger.exception("Traceback for failure")  # includes stack trace
```

Each log file is timestamped and unique per execution. The `setup_logger` function auto-creates directories and prevents duplicate handlers — one `FileHandler` per distinct log file and at most one stdout `StreamHandler` per logger name, no matter how many times it is called.

Log level and root directory come from `logging.level` and `logging.directory` in `config.json` — see [Configuration](configuration.md#application-configuration).

## Stage memory monitoring

Every ETL job logs per-stage memory consumption through a Java `SparkListener`. The
listener design, log fields, committed files, JAR build and deployment steps live in
[Stage memory monitoring](logging/stage-memory.md).
