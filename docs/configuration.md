← [README](../README.md)

# Configuration Reference

Runtime settings live in two JSON files in the project root: `config.json` (Spark, database, logging, data paths and dataset constants) and `supabase_config.json` (Supabase credentials).

## Application Configuration

The environment sections of `config.json`:

```json
{
  "spark": {
    "master": "spark://localhost:7078",
    "app_name_prefix": "geo_etl"
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
| `spark.master` | — | — | Spark cluster master URL |
| `spark.app_name_prefix` | — | `"geo_etl"` | Prefix for Spark application names |
| `database.db_schema` | — | — | PostgreSQL schema name |
| `database.jdbc_driver` | — | — | JDBC driver class |
| `logging.directory` | `"logs"` | — | Root directory for log files |
| `logging.level` | `"INFO"` | — | Log level: DEBUG, INFO, WARNING, ERROR, CRITICAL |
| `data.directory` | `"../GEO_Download/data"` | — | Relative path to CSV data source |

The file is loaded through `AppConfig.from_json()` (`model/config/app_config.py`), which returns nested `SparkConfig`, `DatabaseConfig`, `LoggingConfig` and `DataConfig` objects.

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
    }
  }
}
```

| Section | Contains | Pydantic model |
|---------|----------|----------------|
| `datasets.upload.<script>` | raw table name, CSV folder | `UploadDatasetConfig` |
| `datasets.raw2plain.<script>` | raw + plain table names | `Raw2PlainDatasetConfig` |
| `datasets.single_etl.<script>` | plain/ETL tables, source name, category and country maps, metric labels, lookup tables (`union_etl` instead lists the `*_etl` tables it stacks) | one `<Name>EtlConfig` per script, `UnionEtlConfig` for the union script |

Each script loads its own entry at import time and uses the model's fields directly — there are no per-field aliases:

```python
_cfg = load_dataset(__file__, "single_etl", AvalancheOrgEtlConfig)


def _event_category() -> Column:
    known = F.create_map(*[F.lit(item) for pair in _cfg.category_map.items() for item in pair])
    ...
```

`load_dataset()` (`model/config/datasets.py`) keys the entry by the caller's file stem and validates it against the model — models are frozen and reject unknown keys, so a typo in `config.json` fails when the script is imported. The two computed earthquake helpers (`_us_suffixes`, `_country_name_by_code`) are still derived in the script from the loaded lists and maps.

## Supabase Configuration

Create `supabase_config.json` in the project root with your Supabase credentials:

```json
{
  "username": "postgres.<YOUR-PROJECT-REF>",
  "password": "<YOUR-PASSWORD>",
  "host": "aws-1-eu-west-1.pooler.supabase.com",
  "port": 6543,
  "database": "postgres"
}
```

> **Note:** `6543` is Supabase's **transaction pooler** port (the primary listener uses `5432`); the shipped `supabase_config.json` sets `6543`, and every JDBC URL in this documentation uses the same value.

> **Important:** This file contains database credentials and is excluded from git (listed in `.gitignore`). Never commit it to version control.

`SupabaseConnectionConfig` (`model/config/supabase_config.py`) reads this file and builds the JDBC URL and connection properties used by every upload, transform and DDL execution:

```
jdbc:postgresql://aws-1-eu-west-1.pooler.supabase.com:6543/postgres
```

The properties also carry `prepareThreshold=0`, which disables server-side prepared statements. Supabase's pooler shares server connections between clients, so named statements (`S_1`, …) can otherwise be prepared twice and the write fails with `prepared statement "S_1" already exists`.

See [Data tables](data-tables.md) for the tables it points at and [Usage](usage.md#verify-supabase-connectivity) for a connectivity test.
