← [README](../README.md)

# Data Tables & Supabase

All tables live in the `geo` PostgreSQL schema of Supabase. Every data source is stored as a **pair of tables**:

- **Raw table** (`<name>_raw`) — every column is TEXT, exactly as ingested from the CSV.
- **Plain table** (`<name>`) — typed columns (int, bool, float, date, datetime, timestamp) produced by the raw2plain transform.

DDL is generated from the Pydantic models instead of being written by hand — see [ETL jobs — CREATE TABLE job](etl-jobs.md#create-table-job).

## Registered tables

| Table Name | Pydantic Model | Location | Description |
|------------|----------------|----------|-------------|
| `avalanche_org_raw` | `AvalancheOrgRaw` | `model/raw/` | Avalanche forecast data — all fields as TEXT |
| `avalanche_org` | `AvalancheOrg` | `model/plain/` | Avalanche forecast data — typed (int, bool, datetime, date) |
| `eonet_event_raw` | `EonetEventRaw` | `model/raw/` | NASA EONET event data — all fields as TEXT |
| `eonet_event` | `EonetEvent` | `model/plain/` | NASA EONET event data — typed (float, int, datetime, date) |
| `gdacs_event_raw` | `GdacsEventRaw` | `model/raw/` | GDACS disaster event data — all fields as TEXT |
| `gdacs_event` | `GdacsEvent` | `model/plain/` | GDACS disaster event data — typed (float, int, datetime, bool) |
| `ibtracs_cyclone_raw` | `IbtacsCycloneRaw` | `model/raw/` | IBTrACS cyclone track data — all fields as TEXT |
| `ibtracs_cyclone` | `IbtacsCyclone` | `model/plain/` | IBTrACS cyclone track data — typed (float, int, datetime) |
| `nifc_fire_raw` | `NifcFireRaw` | `model/raw/` | NIFC wildfire incident data — all fields as TEXT |
| `nifc_fire` | `NifcFire` | `model/plain/` | NIFC wildfire data — typed (float, str, datetime) |
| `openmeteo_flood_raw` | `OpenMeteoFloodRaw` | `model/raw/` | OpenMeteo flood monitoring data — all fields as TEXT |
| `openmeteo_flood` | `OpenMeteoFlood` | `model/plain/` | OpenMeteo flood data — typed (float, date) |
| `usgs_earthquake_raw` | `UsgsEarthquakeRaw` | `model/raw/` | USGS earthquake event data — all fields as TEXT |
| `usgs_earthquake` | `UsgsEarthquake` | `model/plain/` | USGS earthquake data — typed (float, int, datetime) |
| `usgs_nwis_raw` | `UsgsNwisRaw` | `model/raw/` | USGS NWIS stream gauge data — all fields as TEXT |
| `usgs_nwis` | `UsgsNwis` | `model/plain/` | USGS NWIS stream gauge data — typed (date, float, double) |
| `usgs_volcano_raw` | `UsgsVolcanoRaw` | `model/raw/` | USGS volcanic activity data — all fields as TEXT |
| `usgs_volcano` | `UsgsVolcano` | `model/plain/` | USGS volcanic activity data — typed (int, timestamp) |
| `data_quality` | `DataQuality` | `model/plain/` | Quality metrics per `file_date` — long format (str, float, timestamp) |

> **Note:** The `_raw` suffix is added automatically. Pass `avalanche_org` to the CLI, not `avalanche_org_raw`.

> **Note:** `data_quality` is metadata rather than a data source — it is created and filled by the [`DATA_QUALITY` job](data-quality.md), not by `CREATE` + `INGESTION`. It appears in the schema after the job's first run, so the table count in [Verifying the schema](#verifying-the-schema) goes from 18 to 19.

Models are registered in `model/__init__.py` via `TableFactory.register()` and looked up by `factory/table_factory.py`.

## Data

Downloaded CSV files from [GEO_Download](../../GEO_Download) are the input. Current sources:

| Source | Raw Table | Final Table | Records | Load Type |
|--------|-----------|-------------|---------|-----------|
| Avalanche_ORG | `geo.avalanche_org_raw` | `geo.avalanche_org` | 3,485 | HISTORICAL |
| EONET | `geo.eonet_event_raw` | `geo.eonet_event` | 109 | HISTORICAL |
| GDACS | `geo.gdacs_event_raw` | `geo.gdacs_event` | 8,031 | HISTORICAL |
| IBTrACS | `geo.ibtracs_cyclone_raw` | `geo.ibtracs_cyclone` | 424 | HISTORICAL |
| NIFC | `geo.nifc_fire_raw` | `geo.nifc_fire` | 190 | HISTORICAL |
| OpenMeteo_Flood | `geo.openmeteo_flood_raw` | `geo.openmeteo_flood` | 4,884 | HISTORICAL |
| USGS_Earthquake | `geo.usgs_earthquake_raw` | `geo.usgs_earthquake` | 7,084 | HISTORICAL |
| USGS_Volcano | `geo.usgs_volcano_raw` | `geo.usgs_volcano` | 24 | HISTORICAL |
| USGS_NWIS | `geo.usgs_nwis_raw` | `geo.usgs_nwis` | 36 | HISTORICAL |

Load type is decided per run — see [ETL jobs — Load type detection](etl-jobs.md#load-type-detection).

## Adding a new table

1. Create a Pydantic model in `model/raw/` (all-TEXT) or `model/plain/` (typed).
2. Register it in `model/__init__.py` with `TableFactory.register("<table_name>", <Model>)`.
3. Register an upload script (`factory/upload_script_factory.py`) and a raw2plain script (`factory/raw2plain_script_factory.py`) for the INGESTION job.
4. Create a corresponding test file under `test/`.
5. Create the table in Supabase — see [Usage — Create tables](usage.md#create-tables-and-run-ingestion).

## Verifying the schema

```bash
conda activate geo_etl_311
python run_pyspark_supabase_test.py
```

Expected output includes the `geo` schema with 18 tables:

```
+------------------+------+
|schema            |tables|
+------------------+------+
|auth              |27    |
|extensions        |2     |
|geo               |18    |
|information_schema|69    |
|public            |2     |
|realtime          |3     |
|reddit            |9     |
|storage           |8     |
|vault             |2     |
+------------------+------+
```

## Dropping tables and schemas

```bash
python run_drop_table.py <table_name>     # -> Table geo.<table_name> dropped.
python run_drop_schema.py <schema_name>   # only works on empty schemas
```

`run_drop_schema.py` refuses to drop a schema that still contains tables:

```
Schema '<schema_name>' contains tables. Drop tables first.
```

See [Usage](usage.md#drop-a-table) for the full commands and expected output.

## Cleaning up the latest file_date

Delete all rows with the most recent `file_date` from both the raw and plain tables — useful for re-running INCREMENTAL tests:

```bash
python run_delete_last_filedate_data.py avalanche_org
```

The script derives the plain table name automatically by stripping the `_raw` suffix (e.g. `avalanche_org_raw` → `avalanche_org`). See [Usage](usage.md#delete-latest-file_date-data).
