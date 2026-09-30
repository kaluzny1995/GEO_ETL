← [README](../README.md)

# Configuration Reference

Runtime settings live in two JSON files in the project root: `config.json` (Spark, database, logging and data paths) and `supabase_config.json` (Supabase credentials).

## Application Configuration

All runtime settings are in `config.json`:

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

`SupabaseConnectionConfig` (`model/config/supabase_config.py`) reads this file and builds the JDBC URL and connection properties used by every upload, transform and DDL execution:

```
jdbc:postgresql://aws-1-eu-west-1.pooler.supabase.com:5432/postgres
```

The properties also carry `prepareThreshold=0`, which disables server-side prepared statements. Supabase's pooler shares server connections between clients, so named statements (`S_1`, …) can otherwise be prepared twice and the write fails with `prepared statement "S_1" already exists`.

See [Data tables](data-tables.md) for the tables it points at and [Usage](usage.md#verify-supabase-connectivity) for a connectivity test.
