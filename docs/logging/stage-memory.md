← [Logging](../logging.md)

# Stage memory monitoring

Every ETL job logs per-stage memory consumption to `logs/memory/stage_memory_<timestamp>.log`.
A Java `SparkListener` fires on each stage completion and writes a single line with JVM heap
usage, Spark execution/storage memory, spill metrics, and GC time.

## How it works

1. `run_etl.py` calls `register_memory_logger(spark, log_file)` after creating the Spark session.
2. A Java `StageMemoryListener` is registered via Py4J (Python cannot subclass the Scala
   `SparkListener` directly, so a Java bridge class `ListenerHelper` handles the cast).
3. On each `onStageCompleted` event the listener snapshots JVM heap (`Runtime.getRuntime()`)
   and Spark memory (`SparkEnv.get().memoryManager()`) and writes a log line.

## Log format

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

## Files

| File | Purpose | Committed? |
|------|---------|------------|
| `utils/spark_listener/StageMemoryListener.java` | Java SparkListener source — logs memory on stage completion | Yes |
| `utils/spark_listener/ListenerHelper.java` | Java bridge source — allows Py4J to register the listener | Yes |
| `utils/spark_listener/StageMemoryListener.class` | Compiled bytecode | Yes |
| `utils/spark_listener/ListenerHelper.class` | Compiled bytecode | Yes |
| `utils/spark_listener/spark-monitor.jar` | Packaged JAR installed into PySpark's `jars/` directory | Yes |
| `utils/spark_monitor.py` | Python wrapper — `register_memory_logger()` | Yes |

The `.java` source, the `.class` bytecode and the `.jar` are all committed. The Docker build context [GEO_E2E](../../../GEO_E2E) keeps `docker/geo_airflow_spark/jars/postgresql-42.7.4.jar` and `docker/geo_airflow_spark/jars/spark-monitor.jar` committed as well, so a fresh clone can build the image without downloading JARs. Rebuild the `.jar` only when the Java source changes (or if the file is missing); JDK 11+ with `javac` and `jar` is required for that step.

## How to build the .jar from source

The `.jar` is committed, so this step is only needed after changing the Java sources or if `utils/spark_listener/spark-monitor.jar` is missing.  To build it, you need JDK 11+ with `javac` and `jar` (a one-time step, or when Java source changes):

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

# 6. Also copy to the image build context (worker + driver inside the image)
cp spark-monitor.jar ../../GEO_E2E/docker/geo_airflow_spark/jars/

# 7. Clean up intermediate files
rm -rf sparkmonitor/
```

**Requirements:** JDK 11+ (`javac` and `jar` commands).

## Deploying to a new environment

When setting up the project on a new machine, use the committed `.jar` (rebuild from source only if the Java sources changed — see above), then:

**1. Rebuild the Airflow/Spark image** (copies `spark-monitor.jar` into `/opt/spark/jars` and the `geo_etl_311` PySpark JARs):

```bash
cd ../GEO_E2E
docker compose build airflow-init
docker compose up -d
```

The Dockerfile builds the same `geo_airflow_spark` image for Airflow and both Spark workers, so no manual image tag or Compose edit is needed.

**2. Copy the JAR to the host-side PySpark** (driver runs on the host, not in Docker):

```bash
conda activate geo_etl_311
JARS_DIR="$(python -c 'import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), "jars"))')"
cp utils/spark_listener/spark-monitor.jar "$JARS_DIR/"
```

This is a one-time step.  After both are done, `register_memory_logger()` will work automatically in all subsequent ETL runs.
