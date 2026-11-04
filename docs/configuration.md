← [README](../README.md)

# Configuration Reference

Runtime settings live in two places: `config.json` in the project root (Spark, database, logging, data paths and dataset constants) and environment variables (Supabase credentials — **set up once in [GEO_E2E](../../GEO_E2E/README.md#1-configure-credentials--mandatory-before-docker-compose-up)**, see [Supabase Credentials](configuration/supabase-credentials.md) below).

## Application Configuration

The environment sections of `config.json`:

```json
{
  "spark": {
    "master": "spark://localhost:7078",
    "app_name_prefix": "geo_etl",
    "driver_memory": "1g",
    "executor_memory": "1g",
    "executor_cores": 1,
    "shuffle_partitions": 8,
    "max_result_size": "256m"
  },
  "database": {
    "db_schema": "geo",
    "jdbc_driver": "org.postgresql.Driver"
  },
  "logging": {
    "directory": "logs",
    "level": "INFO"
  },
  "data": {
    "directory": "../GEO_Download/data"
  }
}
```

| Section | Key | Default | Description |
|---------|-----|---------|-------------|
| `spark.master` | — | — | Spark cluster master URL; `SPARK_MASTER_URL` overrides it at runtime (Compose sets `spark://geo-spark-master:7078`) |
| `spark.app_name_prefix` | — | `"geo_etl"` | Prefix for Spark application names |
| `spark.driver_memory` | `"1g"` | — | PySpark driver heap cap; `SPARK_DRIVER_MEMORY` overrides it |
| `spark.executor_memory` | `"1g"` | — | Executor heap cap; `SPARK_EXECUTOR_MEMORY` overrides it |
| `spark.executor_cores` | `1` | — | Cores per executor; `SPARK_EXECUTOR_CORES` overrides it |
| `spark.shuffle_partitions` | `8` | — | Default shuffle partition count; `SPARK_SHUFFLE_PARTITIONS` overrides it |
| `spark.max_result_size` | `"256m"` | — | Driver result cap; `SPARK_MAX_RESULT_SIZE` overrides it |
| `database.db_schema` | — | — | PostgreSQL schema name |
| `database.jdbc_driver` | — | — | JDBC driver class |
| `logging.directory` | `"logs"` | — | Root directory for log files |
| `logging.level` | `"INFO"` | — | Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL |
| `data.directory` | `"../GEO_Download/data"` | — | Relative path to CSV data source |

The file is loaded through `AppConfig.from_json()` (`model/config/app_config.py`), which returns nested `SparkConfig`, `DatabaseConfig`, `LoggingConfig` and `DataConfig` objects. The `spark.*` memory/parallelism caps are applied by `run_etl.py` to every `SparkSession`; they keep the Airflow scheduler's 4 GB container limit meaningful. Docker Compose supplies the `SPARK_*` overrides from `.env` (see [GEO_E2E — Resource tuning](../../GEO_E2E/docs/configuration.md#resource-tuning-env)).

## Dataset Constants

The `datasets` section of the same file holds every script's wiring and transformation constants, grouped by layer and keyed by the script's file stem (abridged — one entry per script in the real file):

```json
{
  "datasets": {
    "upload": {
      "avalanche_org_upload": {
        "table_name": "avalanche_org_raw",
        "dataset_folder": "Avalanche_ORG"
      }
    },
    "raw2plain": {
      "avalanche_org_raw2plain": {
        "raw_table": "avalanche_org_raw",
        "plain_table": "avalanche_org"
      }
    },
    "single_etl": {
      "avalanche_org_etl": {
        "source_table": "avalanche_org",
        "target_table": "avalanche_org_etl",
        "source_name": "AVALANCHE_ORG",
        "country_code": "US",
        "event_category": "avalanche",
        "metric_name": "magnitude",
        "danger_level_unit": "NAPADS level"
      },
      "union_etl": {
        "target_table": "union_etl",
        "source_tables": ["avalanche_org_etl", "eonet_event_etl", "..."]
      }
    },
    "view": {
      "metrics_avalanche_vw": {
        "source_table": "union_etl",
        "target_view": "metrics_avalanche_vw",
        "query": "WITH avalanche AS (...), other_metric AS (...) SELECT ... UNION ALL SELECT ..."
      }
    }
  }
}
```

| Section | Contains | Pydantic model |
|---------|----------|----------------|
| `datasets.upload.<script>` | raw table name, CSV folder | `UploadDatasetConfig` |
| `datasets.raw2plain.<script>` | raw + plain table names | `Raw2PlainDatasetConfig` |
| `datasets.single_etl.<script>` | plain/ETL tables, source name, category and country maps, metric labels, lookup tables (`union_etl` instead lists the `*_etl` tables it stacks) | one `<Name>EtlConfig` per script, `UnionEtlConfig` for the union script |
| `datasets.view.<script>` | `*_etl` source table, `*_vw` target view and the `SELECT` query the view exposes | `ViewDatasetConfig` |

Each view entry is keyed by the view script's file stem and holds the whole view definition — see [VIEW job](etl_jobs/view.md) and its [Adding a new view script](etl_jobs/view.md#adding-a-new-view-script) recipe (the query is abridged above; the real entry carries the full SQL).

Each script loads its own entry at import time and uses the model's fields directly — there are no per-field aliases:

```python
_cfg = load_dataset(__file__, "single_etl", AvalancheOrgEtlConfig)


def _event_category() -> Column:
    known = literal_map(_cfg.category_map)  # utils.column_helpers
    ...
```

`load_dataset()` (`model/config/datasets.py`) keys the entry by the caller's file stem and validates it against the model — models are frozen and reject unknown keys, so a typo in `config.json` fails when the script is imported. The two computed earthquake helpers (`_us_suffixes`, `_country_name_by_code`) are still derived in the script from the loaded lists and maps.

## Supabase Credentials

The five `SUPABASE_DB_*` variables, the JDBC URL, credential rotation and the
least-privilege role are documented in
[Supabase Credentials](configuration/supabase-credentials.md).
