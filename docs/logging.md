← [README](../README.md)

# Logging

Every script writes structured log files to `logs/`, organized by job type. The logging setup uses the same pattern as [GEO_E2E](../../GEO_E2E) and [GEO_Download](../../GEO_Download).

When called via the INGESTION job, `upload()` and `raw2plain()` receive the ingestion logger, so all logs are consolidated into a single ingestion log file. When called standalone, they create their own separate log files.

**Spark internal logs** (Log4j output from the Spark driver) are captured via a file-descriptor redirect before the JVM starts and appended to the same log file with a `[spark]` prefix after the job completes. This means every `.log` file contains both our application messages and Spark's internal diagnostics in one place.

## Log structure

```
logs/
├── ingestion/
│   └── ingestion_avalanche_org_2026-09-21T10:42:00.777888.log
├── create_table/
│   └── create_table_avalanche_org_raw_2026-09-21T10:30:00.123456.log
├── drop_table/
│   └── drop_table_avalanche_org_raw_2026-09-21T10:35:00.654321.log
├── drop_schema/
│   └── drop_schema_geo_2026-09-21T10:40:00.111222.log
├── delete_last_filedate/
│   └── delete_last_filedate_avalanche_org_raw_2026-09-21T10:42:00.777888.log
├── upload/                          # standalone only
│   └── upload_avalanche_org_raw_2026-09-21T10:42:00.777888.log
├── raw2plain/                       # standalone only
│   └── raw2plain_avalanche_org_2026-09-21T10:45:00.333444.log
├── pyspark_csv_test/
│   └── pyspark_csv_test_2026-09-21T10:45:00.333444.log
└── pyspark_supabase_test/
    └── pyspark_supabase_test_2026-09-21T10:50:00.555666.log
```

## Log format

```
2026-09-24 22:05:45,466 INFO ---- ETL job started: create avalanche_org_raw ----
2026-09-24 22:05:48,472 INFO Connected to Spark: spark://localhost:7078
2026-09-24 22:05:48,473 INFO Spark version: 3.5.5
2026-09-24 22:05:48,473 INFO Creating schema 'geo' if not exists...
2026-09-24 22:05:50,222 INFO ---- Spark internal logs ----
2026-09-24 22:05:50,222 INFO [spark] 26/09/24 22:05:46 WARN Utils: Your hostname ... resolves to a loopback address
2026-09-24 22:05:50,222 INFO [spark] Setting default log level to "WARN".
2026-09-24 22:05:50,222 INFO [spark] 26/09/24 22:05:47 WARN NativeCodeLoader: Unable to load native-hadoop library
2026-09-24 22:05:50,400 INFO Schema 'geo' ready
2026-09-24 22:05:50,400 INFO Generated DDL for geo.avalanche_org_raw
2026-09-24 22:05:50,401 INFO Executing DDL against Supabase...
2026-09-24 22:05:50,500 INFO DDL executed successfully
2026-09-24 22:05:50,500 INFO Table geo.avalanche_org_raw created.
2026-09-24 22:05:50,600 INFO Spark session stopped
```

Lines prefixed with `[spark]` are Spark-internal Log4j output. All other lines are from our Python application code.

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

Each log file is timestamped and unique per execution. The `setup_logger` function auto-creates directories and prevents duplicate handlers.

Log level and root directory come from `logging.level` and `logging.directory` in `config.json` — see [Configuration](configuration.md#application-configuration).

## Stage Memory Monitoring

Every ETL job logs per-stage memory consumption to `logs/memory/stage_memory_<timestamp>.log`.
A Java `SparkListener` fires on each stage completion and writes a single line with JVM heap
usage, Spark execution/storage memory, spill metrics, and GC time.

### How it works

1. `run_etl.py` calls `register_memory_logger(spark, log_file)` after creating the Spark session.
2. A Java `StageMemoryListener` is registered via Py4J (Python cannot subclass the Scala
   `SparkListener` directly, so a Java bridge class `ListenerHelper` handles the cast).
3. On each `onStageCompleted` event the listener snapshots JVM heap (`Runtime.getRuntime()`)
   and Spark memory (`SparkEnv.get().memoryManager()`) and writes a log line.

### Log format

```
2026-09-24 19:30:12 INFO Stage 0 (csv at upload_utils.py:83): 4 tasks | JVM Heap: 72 MB / 1024 MB (7.0%) | Spark Exec=0 MB + Storage=0 MB | MemSpilled=0, DiskSpilled=0, ExecTime=1498ms, GCTime=0ms
2026-09-24 19:30:18 INFO Stage 1 (union at upload_utils.py:85): 8 tasks | JVM Heap: 213 MB / 1024 MB (20.8%) | Spark Exec=0 MB + Storage=0 MB | MemSpilled=0, DiskSpilled=0, ExecTime=5363ms, GCTime=53ms
```

| Field | Description |
|-------|-------------|
| `JVM Heap: X MB / Y MB (Z%)` | Driver JVM heap used vs max |
| `Spark Exec=X MB + Storage=Y MB` | Spark unified memory manager breakdown |
| `MemSpilled` | Bytes spilled from memory to disk |
| `DiskSpilled` | Bytes written to disk spill files |
| `ExecTime` | Total executor run time (ms) |
| `GCTime` | JVM garbage collection time (ms) |

### Files

| File | Purpose | Committed? |
|------|---------|------------|
| `utils/spark_listener/StageMemoryListener.java` | Java SparkListener source — logs memory on stage completion | Yes |
| `utils/spark_listener/ListenerHelper.java` | Java bridge source — allows Py4J to register the listener | Yes |
| `utils/spark_listener/StageMemoryListener.class` | Compiled bytecode | Yes |
| `utils/spark_listener/ListenerHelper.class` | Compiled bytecode | Yes |
| `utils/spark_listener/spark-monitor.jar` | Packaged JAR installed into PySpark's `jars/` directory | No (`*.jar` in `.gitignore`) |
| `utils/spark_monitor.py` | Python wrapper — `register_memory_logger()` | Yes |

The `.java` source and `.class` bytecode are committed.  The `.jar` is excluded by `.gitignore` (`*.jar`) because the same rule also excludes the large third-party JARs in `spark_cluster_image/jars/` (267 MB total).  End users build the `.jar` from source as a one-time setup step (see below) — JDK 11+ with `javac` and `jar` is required only for this step.

### How to build the .jar from source

The `.jar` is excluded from the repository (by the `*.jar` gitignore rule).  To build it, you need JDK 11+ with `javac` and `jar`.  This is a one-time step (or when Java source changes):

```bash
conda activate geo_etl_311

# 1. Find PySpark's JAR directory (contains spark-core, log4j, etc.)
JARS_DIR="$(python -c 'import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"

# 2. Build classpath from all PySpark JARs (Spark, Scala, Log4j, etc.)
CP=$(ls "$JARS_DIR"/*.jar | tr '\n' ':')

# 3. Compile Java sources against the Spark classpath
cd utils/spark_listener
javac -proc:none -cp "$CP" StageMemoryListener.java ListenerHelper.java

# 4. Package .class files into a JAR
mkdir -p sparkmonitor && cp *.class sparkmonitor/
jar cf spark-monitor.jar sparkmonitor/

# 5. Install into PySpark's JAR directory (driver side)
cp spark-monitor.jar "$JARS_DIR/"

# 6. Also copy to Spark image build context (worker side)
cp spark-monitor.jar ../../spark_cluster_image/jars/

# 7. Clean up intermediate files
rm -rf sparkmonitor/
```

**Requirements:** JDK 11+ (`javac` and `jar` commands).

### Deploying to a new environment

When setting up the project on a new machine, first build the `.jar` from source (see above), then:

**1. Rebuild the Spark Docker image** (includes `spark-monitor.jar` for workers):

```bash
cd spark_cluster_image
docker build -t geo_spark_custom .
```

Then update the Docker Compose in [GEO_E2E](../../GEO_E2E) to use `geo_spark_custom` as the image for master and workers, and restart the cluster.

**2. Copy the JAR to the host-side PySpark** (driver runs on the host, not in Docker):

```bash
conda activate geo_etl_311
JARS_DIR="$(python -c 'import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"
cp utils/spark_listener/spark-monitor.jar "$JARS_DIR/"
```

This is a one-time step.  After both are done, `register_memory_logger()` will work automatically in all subsequent ETL runs.
