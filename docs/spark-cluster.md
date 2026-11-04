← [README](../README.md)

# Spark Cluster & PySpark

The Spark standalone cluster is part of the **`geo_airflow_spark`** image built by the Docker Compose stack in [GEO_E2E](../../GEO_E2E). Inside the Compose network the master is `spark://geo-spark-master:7078`; host-side PySpark scripts keep using `spark://localhost:7078`, which is published by Compose. CSV files live in the host `GEO_Download/data` folder bind-mounted at `/opt/geo/GEO_Download/data` in the Airflow and Spark containers.

## Cluster Configuration

| Service | Hostname | Ports | Resources |
|---------|----------|-------|-----------|
| `geo-spark-master` | `geo-spark-master` | 7078 (cluster), 8083 (Web UI) | 1g container limit |
| `geo-spark-worker-1` | `geo-spark-worker-1` | 8084 (Web UI) | 2 cores, 2g advertised, 3g container limit |
| `geo-spark-worker-2` | `geo-spark-worker-2` | 8085 (Web UI) | 2 cores, 2g advertised, 3g container limit |

The master and both workers run the same image (`geo_airflow_spark`); only the container command differs. Worker memory/cores and the container limit are configurable through `SPARK_WORKER_MEMORY`, `SPARK_WORKER_CORES` and `SPARK_WORKER_MEM_LIMIT` in `GEO_E2E/.env`; the container limit must stay above the worker JVM plus its executor. Every Compose service also has `memswap_limit` equal to `mem_limit`, so Spark cannot swap the host to death.

### Starting the cluster

The `docker-compose.yaml` lives in [GEO_E2E](../../GEO_E2E) — build the shared image and start the three services with the command from [Usage — Start the Spark cluster](usage.md#3-start-the-spark-cluster):

```bash
cd ../GEO_E2E
./build-images.sh                   # serial build; docker compose build works too
docker compose up -d geo-spark-master geo-spark-worker-1 geo-spark-worker-2
```

### Starting with the machine

Every long-running service in the compose file carries `restart: unless-stopped`, and the Docker daemon itself is enabled at boot — so once a container has been created and started once, it comes back automatically after a reboot (master, both workers, the Airflow stack), no login or manual `docker compose up` needed. `airflow-init` and `airflow-dag-activate` are one-shot jobs and `airflow-cli` is behind the `debug` profile; none of them restart automatically. `docker compose stop` stays stopped across reboots with `unless-stopped`.

### Spark Web UI

Open http://localhost:8083 in your browser to access the Spark Master Web UI. From there you can:

- **Workers** — see all registered workers, their status, cores, and memory
- **Applications** — monitor running and completed Spark applications
- **Jobs** — view job progress, stages, and tasks for each application
- **Executors** — check executor memory usage, GC stats, and shuffle data

Individual worker Web UIs are available at:
- Worker 1: http://localhost:8084
- Worker 2: http://localhost:8085

## Custom Spark image

GEO_E2E's `docker/geo_airflow_spark/Dockerfile` builds the `geo_airflow_spark` image: Airflow 3.3.2, Anaconda, the `geo_e2e_311` / `geo_download_311` / `geo_etl_311` environments and Spark 3.5.5 copied from `apache/spark:3.5.5`. The same image runs Airflow and the Spark master/workers:

```bash
cd ../GEO_E2E
./build-images.sh                   # serial build; docker compose build works too
docker compose up -d
```

`docker compose restart` does not rebuild the image. The PostgreSQL JDBC and `spark-monitor` JARs are copied at build time from `docker/geo_airflow_spark/jars/` into both `/opt/spark/jars/` and the `geo_etl_311` environment's PySpark `jars/` directory.

## Spark JARs

The JARs in `docker/geo_airflow_spark/jars/` are part of the build context:

| JAR | Version | Purpose |
|-----|---------|---------|
| `postgresql` | 42.7.4 | PostgreSQL JDBC driver for Supabase connectivity |
| `spark-monitor` | — | Stage memory logging SparkListener (built from `utils/spark_listener/`, see [Logging — Stage memory monitoring](logging/stage-memory.md)) |

The optional S3A JARs (`hadoop-aws`, `aws-java-sdk-bundle`) are not needed by this solution and are excluded from the image build context.

The PostgreSQL JDBC driver must also be available locally for host-side PySpark drivers. Copy it to the PySpark installation:

```bash
cp ../GEO_E2E/docker/geo_airflow_spark/jars/postgresql-42.7.4.jar $(python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))")
```

## Connecting from PySpark

```python
import findspark

findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder.master("spark://localhost:7078").appName("my_etl_job").getOrCreate()

# Read CSVs
df = spark.read.csv("/path/to/GEO_Download/data/USGS_Earthquake/*.csv", header=True)
df.show()
```

Inside the `geo_airflow_spark` containers the same code uses `spark://geo-spark-master:7078` (GEO_ETL reads it from `SPARK_MASTER_URL`) and `/opt/geo/GEO_Download/data`.

> **Note:** For host-side runs the data path must be accessible from both the driver (host) and workers (Docker containers); mount it at the same absolute path. The Dockerized pipeline bind-mounts the host `GEO_Download/data` folder (`GEO_DATA_DIR`) at `/opt/geo/GEO_Download/data` for the Airflow driver and every Spark executor, so host and containers use the same path.

### Verify cluster connectivity

```bash
conda activate geo_etl_311
python run_pyspark_csv_test.py
```

Expected log file content (`logs/pyspark_csv_test/pyspark_csv_test_<timestamp>.log` — the script logs to the file only):

```
Connected to: spark://localhost:7078
Spark version: 3.5.5
Data path:     /path/to/GEO_Download/data

  Avalanche_ORG                  -> 33 CSV file(s)
  EONET                          -> 32 CSV file(s)
  GDACS                          -> 33 CSV file(s)
  ...
```

The CSV counts are illustrative — they grow with every [GEO_Download](../../GEO_Download) run.
