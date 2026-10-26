← [README](../README.md)

# Spark Cluster & PySpark

PySpark scripts run on the host and connect to a Spark standalone cluster managed by Docker Compose in [GEO_E2E](../../GEO_E2E). The cluster exposes a master at `spark://localhost:7078` with two workers; CSV files from [GEO_Download](../../GEO_Download) are mounted read-only into every node at the same absolute path.

## Cluster Configuration

| Service | Container | Hostname | Ports | Resources |
|---------|-----------|----------|-------|-----------|
| Spark Master | `geo_spark_master` | `geo-spark-master` | 7078 (cluster), 8083 (Web UI) | — |
| Spark Worker 1 | `geo_spark_worker_1` | — | 8084 (Web UI) | 2 cores, 2g RAM |
| Spark Worker 2 | `geo_spark_worker_2` | — | 8085 (Web UI) | 2 cores, 2g RAM |

### Starting the cluster

The `docker-compose.yaml` lives in [GEO_E2E](../../GEO_E2E) — start the three services with the command from [Usage — Start the Spark cluster](usage.md#2-start-the-spark-cluster).

### Starting with the machine

Every long-running service in the compose file carries `restart: always`, and the Docker daemon itself is enabled at boot — so once a container has been created and started once, it comes back automatically after a reboot (master, both workers, the Airflow stack), no login or manual `docker compose up` needed. `airflow-init` (one-shot init) and `airflow-cli` (debug profile) are deliberately excluded. Note that with `restart: always` a manual `docker compose stop` only sticks until the next reboot — switch the policy to `unless-stopped` if a stopped service should stay stopped.

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

`spark_cluster_image/` holds a Dockerfile based on `apache/spark:3.5.5` that adds the Hadoop AWS and PostgreSQL JARs:

```bash
cd spark_cluster_image
docker build -t geo_spark_custom .
```

GEO_E2E's `docker-compose.yaml` builds this image itself (`build: ../GEO_ETL/spark_cluster_image` for master and both workers), so after changing the Dockerfile run `docker compose build` in GEO_E2E and recreate the cluster — no manual tagging or compose edits needed. The `geo_spark_custom` tag above is only for other compose files or registries that expect a pre-built image name.

## Spark JARs

The `spark_cluster_image/jars/` directory is excluded from git (listed in `.gitignore`). After cloning, download the required JARs manually:

```bash
cd spark_cluster_image/jars
curl -fSL -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
curl -fSL -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
curl -fSL -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
```

| JAR | Version | Purpose |
|-----|---------|---------|
| `hadoop-aws` | 3.3.4 | S3A filesystem client (for future S3/MinIO access) |
| `aws-java-sdk-bundle` | 1.12.262 | AWS SDK dependency required by hadoop-aws |
| `postgresql` | 42.7.4 | PostgreSQL JDBC driver for Supabase connectivity |
| `spark-monitor` | — | Stage memory logging SparkListener (built from `utils/spark_listener/`, see [Logging — Stage memory monitoring](logging.md#stage-memory-monitoring)) |

These JARs are copied into the Spark Docker image during build.

The PostgreSQL JDBC driver must also be available locally for the PySpark driver (which runs on the host). Copy it to the PySpark installation:

```bash
cp spark_cluster_image/jars/postgresql-42.7.4.jar $(python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))")
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

> **Note:** The data path must be accessible from both the driver (host) and workers (Docker containers). The Docker Compose in GEO_E2E mounts `GEO_DATA_DIR` at the same absolute path on both sides.

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
