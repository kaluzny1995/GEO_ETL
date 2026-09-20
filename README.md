# GEO_ETL

GIS Data ETL Pipeline — PySpark-based extract-transform-load processes for geospatial data.

## Overview

GEO_ETL is the transformation layer in the GIS data pipeline. It reads raw CSV files produced by [GEO_Download](../GEO_Download), applies transformations and enrichment using PySpark, and outputs clean, analysis-ready datasets.

PySpark scripts run locally and connect to a Spark standalone cluster managed by Docker Compose (configured in [GEO_E2E](../GEO_E2E)). The cluster exposes a master at `spark://localhost:7078` with two workers.

## Features

- **Spark standalone cluster** — 1 master + 2 workers via Docker Compose (in GEO_E2E)
- **Shared data access** — CSV files from GEO_Download mounted read-only into all Spark nodes
- **Supabase connectivity** — PostgreSQL JDBC driver for reading/writing to Supabase
- **Custom Spark image** — based on `apache/spark:3.5.5` with Hadoop AWS + PostgreSQL JARs
- **Local PySpark client** — scripts run on the host, connecting to the cluster remotely

## Tech Stack

| Component        | Technology                          |
|------------------|-------------------------------------|
| Language         | Python 3.11                         |
| Processing       | PySpark 3.5.5                       |
| Cluster          | Apache Spark 3.5.5 standalone       |
| Database         | PostgreSQL (Supabase) via JDBC      |
| Data Validation  | Pydantic >= 2.0.0                   |
| HTTP Client      | requests >= 2.31.0                  |
| Testing          | pytest >= 7.0.0                     |
| Environment      | Conda                               |
| Containers       | Docker + Docker Compose             |

## Project Structure

```
GEO_ETL/
├── environment.yml                   # Conda environment definition
├── supabase_config.json              # Supabase credentials (not in git)
├── run_pyspark_csv_test.py           # Cluster verification script
├── run_pyspark_supabase_test.py      # Supabase connection test script
├── spark/
│   ├── Dockerfile                    # Custom Spark image (apache/spark:3.5.5 + JARs)
│   └── jars/
│       ├── hadoop-aws-3.3.4.jar
│       ├── aws-java-sdk-bundle-1.12.262.jar
│       └── postgresql-42.7.4.jar
└── README.md
```

## Spark JARs

The `spark/jars/` directory is excluded from git (listed in `.gitignore`). After cloning, download the required JARs manually:

```bash
cd spark/jars
curl -fSL -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
curl -fSL -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar
curl -fSL -O https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar
```

| JAR | Version | Purpose |
|-----|---------|---------|
| `hadoop-aws` | 3.3.4 | S3A filesystem client (for future S3/MinIO access) |
| `aws-java-sdk-bundle` | 1.12.262 | AWS SDK dependency required by hadoop-aws |
| `postgresql` | 42.7.4 | PostgreSQL JDBC driver for Supabase connectivity |

These JARs are copied into the Spark Docker image during build.

The PostgreSQL JDBC driver must also be available locally for the PySpark driver (which runs on the host). Copy it to the PySpark installation:

```bash
cp spark/jars/postgresql-42.7.4.jar $(python -c "import pyspark, os; print(os.path.join(os.path.dirname(pyspark.__file__), 'jars'))")
```

## Prerequisites

- **Conda** — for environment management
- **Java 11+** — required by PySpark
- **Docker** — for running the Spark cluster
- **GEO_Download** — must be a sibling directory (source of raw CSVs)
- **GEO_E2E** — hosts the `docker-compose.yaml` that runs the Spark cluster

## Setup

### 1. Create the Conda environment

```bash
conda env create -f environment.yml
conda activate geo_etl_311
```

### 2. Start the Spark cluster

The cluster is managed by Docker Compose in [GEO_E2E](../GEO_E2E). Make sure the `.env` file there is configured with the correct `GEO_DATA_DIR` path, then:

```bash
cd ../GEO_E2E
docker compose up -d geo_spark_master geo_spark_worker_1 geo_spark_worker_2
```

### 3. Verify cluster connectivity

```bash
conda activate geo_etl_311
python run_pyspark_csv_test.py
```

Expected output:

```
Connected to: spark://localhost:7078
Spark version: 3.5.5
Data path:     /path/to/GEO_Download/data

  Avalanche_ORG                  -> 19 CSV file(s)
  EONET                          -> 18 CSV file(s)
  GDACS                          -> 19 CSV file(s)
  ...
```

## Supabase Configuration

Create `supabase_config.json` in the project root with your Supabase credentials:

```json
{
  "username": "postgres.<YOUR-PROJECT-REF>",
  "password": "<YOUR-PASSWORD>",
  "host": "aws-1-eu-west-1.pooler.supabase.com",
  "port": 5432,
  "database": "postgres"
}
```

> **Important:** This file contains database credentials and is excluded from git (listed in `.gitignore`). Never commit it to version control.

### Test Supabase connectivity

```bash
conda activate geo_etl_311
python run_pyspark_supabase_test.py
```

Expected output:

```
Connected to:   spark://localhost:7078
Spark version:  3.5.5
Supabase host:  aws-1-eu-west-1.pooler.supabase.com
JDBC URL:       jdbc:postgresql://aws-1-eu-west-1.pooler.supabase.com:5432/postgres

+------------------+
|schema_name       |
+------------------+
|auth              |
|extensions        |
|graphql           |
|graphql_public    |
|information_schema|
|pgbouncer         |
|public            |
|realtime          |
|reddit            |
|storage           |
|vault             |
+------------------+

Done.
```

## Cluster Configuration

| Service | Container | Hostname | Ports | Resources |
|---------|-----------|----------|-------|-----------|
| Spark Master | `geo_spark_master` | `geo-spark-master` | 7078 (cluster), 8083 (Web UI) | — |
| Spark Worker 1 | `geo_spark_worker_1` | — | 8084 (Web UI) | 2 cores, 2g RAM |
| Spark Worker 2 | `geo_spark_worker_2` | — | 8085 (Web UI) | 2 cores, 2g RAM |

**Spark Web UI:** http://localhost:8083

## Connecting from PySpark

```python
import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .master("spark://localhost:7078") \
    .appName("my_etl_job") \
    .getOrCreate()

# Read CSVs
df = spark.read.csv("/path/to/GEO_Download/data/USGS_Earthquake/*.csv", header=True)
df.show()
```

> **Note:** The data path must be accessible from both the driver (host) and workers (Docker containers). The Docker Compose in GEO_E2E mounts `GEO_DATA_DIR` at the same absolute path on both sides.

## Data

Downloaded CSV files from [GEO_Download](../GEO_Download) are the input. Current sources:

| Source | Description |
|--------|-------------|
| Avalanche_ORG | Avalanche data |
| EONET | NASA natural events |
| GDACS | Global disasters |
| IBTrACS | Tropical cyclone best-track |
| NIFC | US wildland fires |
| OpenMeteo_Flood | River discharge data |
| USGS_Earthquake | Seismic events |
| USGS_NWIS | US streamflow |
| USGS_Volcano | US volcano alerts |

## Related Projects

- **GEO_Download** — downloads raw CSV data from public APIs
- **GEO_E2E** — orchestrates downloads via Celery + RabbitMQ; also runs the Spark cluster via Docker Compose
