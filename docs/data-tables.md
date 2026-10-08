← [README](../README.md)

# Data Tables & Supabase

All tables live in the `geo` PostgreSQL schema of Supabase. Every data source is stored as a **pair of tables**:

- **Raw table** (`<name>_raw`) — every column is TEXT, exactly as ingested from the CSV.
- **Plain table** (`<name>`) — typed columns (int, bool, float, date, datetime) produced by the raw2plain transform.
- **ETL table** (`<name>_etl`) — canonical event row (same 21 columns for every source) produced by the [`SINGLE_ETL` job](etl_jobs/single-etl.md), ready to be merged with the other `*_etl` tables.
- **Union table** (`union_etl`) — every `*_etl` table stacked into one by the [`union_etl` script](etl_jobs/single-etl.md#union_etl) of the [`SINGLE_ETL` job](etl_jobs/single-etl.md); `source_name` still says which source a row came from.

DDL is generated from the Pydantic models instead of being written by hand — see [ETL jobs — CREATE TABLE job](etl_jobs/create-table.md).

## Registered tables

| Table Name | Pydantic Model | Location | Description |
|------------|----------------|----------|-------------|
| `avalanche_org_raw` | `AvalancheOrgRaw` | `model/raw/` | Avalanche forecast data — all fields as TEXT |
| `avalanche_org` | `AvalancheOrg` | `model/plain/` | Avalanche forecast data — typed (int, bool, datetime, date) |
| `avalanche_org_etl` | `AvalancheOrgEtl` | `model/etl/` | Avalanche zone/day in the canonical event structure (str, float, datetime) |
| `eonet_event_raw` | `EonetEventRaw` | `model/raw/` | NASA EONET event data — all fields as TEXT |
| `eonet_event` | `EonetEvent` | `model/plain/` | NASA EONET event data — typed (float, int, datetime, date) |
| `eonet_event_etl` | `EonetEventEtl` | `model/etl/` | NASA EONET event in the canonical event structure (str, float, datetime) |
| `gdacs_event_raw` | `GdacsEventRaw` | `model/raw/` | GDACS disaster event data — all fields as TEXT |
| `gdacs_event` | `GdacsEvent` | `model/plain/` | GDACS disaster event data — typed (float, int, datetime, bool) |
| `gdacs_event_etl` | `GdacsEventEtl` | `model/etl/` | GDACS disaster event snapshot in the canonical event structure (str, float, datetime) |
| `ibtracs_cyclone_raw` | `IbtacsCycloneRaw` | `model/raw/` | IBTrACS cyclone track data — all fields as TEXT |
| `ibtracs_cyclone` | `IbtacsCyclone` | `model/plain/` | IBTrACS cyclone track data — typed (float, int, datetime) |
| `ibtracs_cyclone_etl` | `IbtracsCycloneEtl` | `model/etl/` | IBTrACS best-track position in the canonical event structure (str, float, datetime) |
| `nifc_fire_raw` | `NifcFireRaw` | `model/raw/` | NIFC wildfire incident data — all fields as TEXT |
| `nifc_fire` | `NifcFire` | `model/plain/` | NIFC wildfire data — typed (float, str, datetime) |
| `nifc_fire_etl` | `NifcFireEtl` | `model/etl/` | NIFC fire perimeter snapshot in the canonical event structure (str, float, datetime) |
| `openmeteo_flood_raw` | `OpenMeteoFloodRaw` | `model/raw/` | OpenMeteo flood monitoring data — all fields as TEXT |
| `openmeteo_flood` | `OpenMeteoFlood` | `model/plain/` | OpenMeteo flood data — typed (float, date) |
| `openmeteo_flood_etl` | `OpenMeteoFloodEtl` | `model/etl/` | OpenMeteo river discharge snapshot in the canonical event structure (str, float, datetime) |
| `usgs_earthquake_raw` | `UsgsEarthquakeRaw` | `model/raw/` | USGS earthquake event data — all fields as TEXT |
| `usgs_earthquake` | `UsgsEarthquake` | `model/plain/` | USGS earthquake data — typed (float, int, datetime) |
| `usgs_earthquake_etl` | `UsgsEarthquakeEtl` | `model/etl/` | USGS seismic event in the canonical event structure (str, float, datetime) |
| `usgs_nwis_raw` | `UsgsNwisRaw` | `model/raw/` | USGS NWIS stream gauge data — all fields as TEXT |
| `usgs_nwis` | `UsgsNwis` | `model/plain/` | USGS NWIS stream gauge data — typed (date, float) |
| `usgs_nwis_etl` | `UsgsNwisEtl` | `model/etl/` | USGS stream gauge reading snapshot in the canonical event structure (str, float, datetime) |
| `usgs_volcano_raw` | `UsgsVolcanoRaw` | `model/raw/` | USGS volcanic activity data — all fields as TEXT |
| `usgs_volcano` | `UsgsVolcano` | `model/plain/` | USGS volcanic activity data — typed (int, float, datetime) |
| `usgs_volcano_etl` | `UsgsVolcanoEtl` | `model/etl/` | USGS volcanic activity notice in the canonical event structure (str, float, datetime) |
| `union_etl` | `UnionEtl` | `model/etl/` | Every `*_etl` table stacked into one by the `union_etl` script — the canonical 21 columns, `source_name` identifies the source |
| `data_quality` | `DataQuality` | `model/other/` | Quality metrics per `file_date` — long format (str, float, datetime) |

> **Note:** One `CREATE` run builds exactly one table — the raw and the plain table need their own run (`python run_etl.py CREATE <table_name>_raw`, then `python run_etl.py CREATE <table_name>`). Only `INGESTION` appends the `_raw` suffix itself: pass `<table_name>` to it, not `<table_name>_raw`.

> **Note:** `data_quality` is metadata rather than a data source — it is created and filled by the [`DATA_QUALITY` job](etl_jobs/data-quality.md), not by `CREATE` + `INGESTION`. It appears in the schema after the job's first run, so the table count in [Verifying the schema](#verifying-the-schema) goes from 28 to 29.

Models are registered in `model/__init__.py` via `TableFactory.register()` and looked up by `factory/table_factory.py`.

## Data

Downloaded CSV files from [GEO_Download](../../GEO_Download) are the input. Current sources:

| Source | Raw Table | Final Table | Records | Load Type |
|--------|-----------|-------------|---------|-----------|
| Avalanche_ORG | `geo.avalanche_org_raw` | `geo.avalanche_org` | 4,824 | HISTORICAL |
| EONET | `geo.eonet_event_raw` | `geo.eonet_event` | 145 | HISTORICAL |
| GDACS | `geo.gdacs_event_raw` | `geo.gdacs_event` | 9,118 | HISTORICAL |
| IBTrACS | `geo.ibtracs_cyclone_raw` | `geo.ibtracs_cyclone` | 897 | HISTORICAL |
| NIFC | `geo.nifc_fire_raw` | `geo.nifc_fire` | 233 | HISTORICAL |
| OpenMeteo_Flood | `geo.openmeteo_flood_raw` | `geo.openmeteo_flood` | 7,326 | HISTORICAL |
| USGS_Earthquake | `geo.usgs_earthquake_raw` | `geo.usgs_earthquake` | 8,725 | HISTORICAL |
| USGS_Volcano | `geo.usgs_volcano_raw` | `geo.usgs_volcano` | 56 | HISTORICAL |
| USGS_NWIS | `geo.usgs_nwis_raw` | `geo.usgs_nwis` | 54 | HISTORICAL |

Load type is decided per run — see [ETL jobs — Load type detection](etl_jobs/load-types.md).

## Adding a new table

1. Create a Pydantic model in `model/raw/` (all-TEXT), `model/plain/` (typed) or `model/etl/` (canonical event row).
2. Register it in `model/__init__.py` with `TableFactory.register("<table_name>", <Model>)`.
3. Register an upload script (`factory/upload_script_factory.py`) and a raw2plain script (`factory/raw2plain_script_factory.py`) for the INGESTION job.
4. Create a corresponding test file under `test/`.
5. Create the table in Supabase — see [Usage — Create tables](usage.md#create-tables-and-run-ingestion).

## Verifying the schema

`python run_pyspark_supabase_test.py` lists every schema and its table count — `geo` should show the registered tables. Full command and expected output: [Usage — Verify Supabase connectivity](usage.md#verify-supabase-connectivity).

## Dropping tables and schemas

`run_drop_table.py` drops one table; `run_drop_schema.py` drops an empty schema and refuses one that still contains tables. Commands and expected output: [Usage — Drop a table](usage.md#drop-a-table) · [Usage — Drop a schema](usage.md#drop-a-schema).

## Cleaning up the latest file_date

Re-running an INCREMENTAL test needs the latest `file_date` removed from both the raw and plain tables; pass the plain table name. Command and expected output: [Usage — Delete latest file_date data](usage.md#delete-latest-file_date-data).
