← [README](../README.md)

# Data Tables & Supabase

All tables live in the `geo` PostgreSQL schema of Supabase. Every data source is stored as a typed table:

- **Plain table** (`<name>`) — typed columns (int, bool, float, date, datetime) produced directly by the `INGESTION` job from the source CSVs.
- **ETL table** (`<name>_etl`) — canonical event row (same 21 columns for every source) produced by the [`SINGLE_ETL` job](etl_jobs/single-etl.md), deduplicated to one row per natural event per `file_date` (the `dedup_keys` configured for each script), ready to be merged with the other `*_etl` tables.
- **Union table** (`union_etl`) — every `*_etl` table stacked into one by the [`union_etl` script](etl_jobs/single-etl/union-etl.md) of the [`SINGLE_ETL` job](etl_jobs/single-etl.md); `source_name` says which of the nine tables a row came from — one distinct label per table (incl. `USGS_EARTHQUAKE`, `USGS_NWIS`, `USGS_VOLCANO`), which is also what keeps incremental loads per source.
- **View** (`<name>_vw`) — a SQL view over one `*_etl` table, defined by the [`VIEW` job](etl_jobs/view.md) from a query kept in `config.json`. It stores no rows (the database recomputes it on every read), is always redefined in full (`HISTORICAL` only) and needs no `CREATE` run — it is not registered in `TableFactory`.

DDL is generated from the Pydantic models instead of being written by hand — see [ETL jobs — CREATE TABLE job](etl_jobs/create-table.md).

## Registered tables

| Table Name | Pydantic Model | Location | Description |
|------------|----------------|----------|-------------|
| `avalanche_org` | `AvalancheOrg` | `model/plain/` | Avalanche forecast data — typed (int, bool, datetime, date) |
| `avalanche_org_etl` | `AvalancheOrgEtl` | `model/etl/` | Avalanche zone/day in the canonical event structure (str, float, datetime) |
| `eonet_event` | `EonetEvent` | `model/plain/` | NASA EONET event data — typed (float, int, datetime, date) |
| `eonet_event_etl` | `EonetEventEtl` | `model/etl/` | NASA EONET event in the canonical event structure (str, float, datetime) |
| `gdacs_event` | `GdacsEvent` | `model/plain/` | GDACS disaster event data — typed (float, int, datetime, bool) |
| `gdacs_event_etl` | `GdacsEventEtl` | `model/etl/` | GDACS disaster event snapshot in the canonical event structure (str, float, datetime) |
| `ibtracs_cyclone` | `IbtacsCyclone` | `model/plain/` | IBTrACS cyclone track data — typed (float, int, datetime) |
| `ibtracs_cyclone_etl` | `IbtracsCycloneEtl` | `model/etl/` | IBTrACS best-track position in the canonical event structure (str, float, datetime) |
| `nifc_fire` | `NifcFire` | `model/plain/` | NIFC wildfire data — typed (float, str, datetime) |
| `nifc_fire_etl` | `NifcFireEtl` | `model/etl/` | NIFC fire perimeter snapshot in the canonical event structure (str, float, datetime) |
| `openmeteo_flood` | `OpenMeteoFlood` | `model/plain/` | OpenMeteo flood data — typed (float, date) |
| `openmeteo_flood_etl` | `OpenMeteoFloodEtl` | `model/etl/` | OpenMeteo river discharge snapshot in the canonical event structure (str, float, datetime) |
| `usgs_earthquake` | `UsgsEarthquake` | `model/plain/` | USGS earthquake data — typed (float, int, datetime) |
| `usgs_earthquake_etl` | `UsgsEarthquakeEtl` | `model/etl/` | USGS seismic event in the canonical event structure (str, float, datetime) |
| `usgs_nwis` | `UsgsNwis` | `model/plain/` | USGS NWIS stream gauge data — typed (date, float) |
| `usgs_nwis_etl` | `UsgsNwisEtl` | `model/etl/` | USGS stream gauge reading snapshot in the canonical event structure (str, float, datetime) |
| `usgs_volcano` | `UsgsVolcano` | `model/plain/` | USGS volcanic activity data — typed (int, float, datetime) |
| `usgs_volcano_etl` | `UsgsVolcanoEtl` | `model/etl/` | USGS volcanic activity notice in the canonical event structure (str, float, datetime) |
| `union_etl` | `UnionEtl` | `model/etl/` | Every `*_etl` table stacked into one by the `union_etl` script — the canonical 21 columns, `source_name` identifies the source table (one distinct label per table) |
| `data_quality` | `DataQuality` | `model/other/` | Quality metrics per `file_date` (plus the whole-table `overall` snapshot) — long format (str, float, datetime) |
| `data_source_files` | `DataSourceFile` | `model/other/` | CSV inventory published by the `DATA_SOURCE_FILE` job — one row per real CSV file (str, int, float) |
| `data_source_sources` | `DataSourceSource` | `model/other/` | Source-folder registry published by the `DATA_SOURCE_FILE` job — one row per source (str) |

> **Note:** One `CREATE` run builds exactly one table — create the plain table and its `*_etl` table with separate runs. `INGESTION` takes the plain table name and writes directly to it.

> **Note:** `data_quality`, `data_source_files` and `data_source_sources` are metadata rather than data sources — they are created and filled by the [`DATA_QUALITY`](etl_jobs/data-quality.md) and [`DATA_SOURCE_FILE`](etl_jobs/data-source-file.md) jobs, not by `CREATE` + `INGESTION` table creation. They appear in the schema after their first job run, so the table count in [Verifying the schema](#verifying-the-schema) goes from 19 to 22.

Models are registered in `model/__init__.py` via `TableFactory.register()` and looked up by `factory/table_factory.py`.

> `DataSourceFile` and `DataSourceSource` are imported for their Spark schemas but are **not** registered in `TableFactory`: the CSV metadata publisher owns the table DDL, including the composite `(source_name, file_name)` file primary key and the `source_name` source-registry key.

## Views

Views are not tables: they are defined by the [`VIEW` job](etl_jobs/view.md) (no `CREATE` run, no `TableFactory` registration) and always `HISTORICAL` — every run replaces the whole definition.

| View | Source | Description |
|------|--------|-------------|
| `metrics_avalanche_vw` | `geo.union_etl` | Every measured avalanche-category metric flattened into one row per (name, value, unit): the row's own `metric_name` / `metric_value` / `metric_unit` (kept only when it carries a value), appended rows for the numeric entries of its `other_metrics` JSON with their companion `<name>_unit` units, and the row's `file_date` stamp cast to `timestamp` (trailing column) — see [VIEW job — metrics_avalanche_vw](etl_jobs/view/metrics-views.md#metrics_avalanche_vw) |
| `metrics_drought_vw` | `geo.union_etl` | The drought twin of the avalanche view (`event_category = 'drought'`): GDACS's own `area` / `km²` metric per row plus the numeric `other_metrics` explosion (currently NULL for GDACS, kept for other sources), with the trailing `file_date` timestamp — see [VIEW job — metrics_drought_vw](etl_jobs/view/metrics-views.md#metrics_drought_vw) |
| `metrics_earthquake_vw` | `geo.union_etl` | The earthquake twin (`event_category = 'earthquake'`): each row's own `magnitude` / `Mw` metric (rare `mh`/`Mi`/`mun`/`ms_vx` scales excluded) plus `depth` / `nst` rows exploded from its numeric `other_metrics` JSON (with companion `<name>_unit` units), trailing `file_date` timestamp — see [VIEW job — metrics_earthquake_vw](etl_jobs/view/metrics-views.md#metrics_earthquake_vw) |
| `metrics_fire_vw` | `geo.union_etl` | The fire twin (`event_category = 'fire'`): each row's own `area` / `km²` metric plus `percent_contained` / `%` (NIFC) and `geometry_count` / NULL-unit (EONET) rows exploded from its numeric `other_metrics` JSON, trailing `file_date` timestamp — see [VIEW job — metrics_fire_vw](etl_jobs/view/metrics-views.md#metrics_fire_vw) |
| `metrics_flood_vw` | `geo.union_etl` | The flood twin (`event_category = 'flood'`), whitelisted to five metrics: own `discharge` / `m³/s` (OPENMETEO, USGS_NWIS) and `magnitude` (GDACS) plus exploded `threshold` / `m³/s`, `baseline_days` and `exceedance_ratio` (NULL unit) from its numeric `other_metrics` JSON — percentile/ensemble/stage keys are filtered out; trailing `file_date` timestamp — see [VIEW job — metrics_flood_vw](etl_jobs/view/metrics-views.md#metrics_flood_vw) |
| `metrics_iceberg_vw` | `geo.union_etl` | The iceberg twin (`event_category = 'iceberg'`): each EONET row's own `surface` / `N/m²` metric plus its `geometry_count` / NULL-unit row exploded from `other_metrics`, trailing `file_date` timestamp — see [VIEW job — metrics_iceberg_vw](etl_jobs/view/metrics-views.md#metrics_iceberg_vw) |
| `metrics_storm_vw` | `geo.union_etl` | The storm twin (`event_category = 'storm'`): own `wind speed` / `km/h` (IBTRACS, GDACS, EONET) plus the exploded IBTRACS block (`pressure` / `mb`, `speed` / `km/h`, `direction` / `degrees`, `gust` / `km/h`, `dist2land` / `landfall` / `km`) and EONET `geometry_count` / NULL unit, trailing `file_date` timestamp — see [VIEW job — metrics_storm_vw](etl_jobs/view/metrics-views.md#metrics_storm_vw) |
| `metrics_volcano_vw` | `geo.union_etl` | The volcano twin (`event_category = 'volcano'`): own `vei` / `VEI` (USGS) and `magnitude` / NULL (GDACS) metrics plus exploded `vnum` / `Smithsonian GVP`, `casualties` / `people` and `damage` / `million USD` from USGS notices, trailing `file_date` timestamp — see [VIEW job — metrics_volcano_vw](etl_jobs/view/metrics-views.md#metrics_volcano_vw) |
| `phenomenons_us_vw` | `geo.union_etl` | Denormalized US-only rows (`country_code = 'US'`) for Looker Studio map visualization: capitalized categories, `event_ndesc` display name (name + `:\n\n` + description), map coordinates/place fields, categorical `danger_rating`, own metric columns, `other_metrics` as `key: value (unit); ...` text, coalesced `event_start_date` and raw `event_end_date` — see [VIEW job — phenomenons_us_vw](etl_jobs/view/phenomenon-views.md#phenomenons_us_vw) |
| `phenomenons_ca_vw` | `geo.union_etl` | The Canada twin of the US map view (identical 15-column display query, `country_code = 'CA'`): 809 rows of Canadian phenomena for Looker Studio — see [VIEW job — phenomenons_ca_vw](etl_jobs/view/phenomenon-views.md#phenomenons_ca_vw) |
| `phenomenons_cs_amer_vw` | `geo.union_etl` | The Americas twin of the US map view (identical `SELECT`, `country_code IN (<Americas ISO list minus US/CA>)`): 4,506 rows across Central/South America & the Caribbean for Looker Studio — see [VIEW job — phenomenons_cs_amer_vw](etl_jobs/view/phenomenon-views.md#phenomenons_cs_amer_vw) |
| `phenomenons_ru_vw` | `geo.union_etl` | The Russia twin of the US map view (identical 15-column display query, `country_code = 'RU'`): 995 rows of Russian phenomena for Looker Studio — see [VIEW job — phenomenons_ru_vw](etl_jobs/view/phenomenon-views.md#phenomenons_ru_vw) |
| `phenomenons_europe_vw` | `geo.union_etl` | The Europe twin of the US map view (identical `SELECT`, `country_code IN (<Europe ISO list minus RU>)`): 2,614 rows across strict M49 Europe for Looker Studio — see [VIEW job — phenomenons_europe_vw](etl_jobs/view/phenomenon-views.md#phenomenons_europe_vw) |
| `phenomenons_asia_vw` | `geo.union_etl` | The Asia twin of the US map view (identical `SELECT`, `country_code IN (<Asia ISO list minus RU>)`): 4,601 rows across strict M49 Asia for Looker Studio — see [VIEW job — phenomenons_asia_vw](etl_jobs/view/phenomenon-views.md#phenomenons_asia_vw) |
| `phenomenons_africa_vw` | `geo.union_etl` | The Africa twin of the US map view (identical `SELECT`, `country_code IN (<Africa ISO list>)`): 7,968 rows across M49 Africa (54 AU members + territories) for Looker Studio — see [VIEW job — phenomenons_africa_vw](etl_jobs/view/phenomenon-views.md#phenomenons_africa_vw) |
| `phenomenons_aus_oc_vw` | `geo.union_etl` | The Australia/Oceania twin of the US map view (identical `SELECT`, `country_code IN (<Oceania ISO list>)`): 3,890 rows across M49 Oceania's four sub-regions for Looker Studio — see [VIEW job — phenomenons_aus_oc_vw](etl_jobs/view/phenomenon-views.md#phenomenons_aus_oc_vw) |
| `phenomenons_waters_vw` | `geo.union_etl` | The water twin of the US map view (identical `SELECT`; heuristic = geocode guard ∧ (`'ZZ'` ∪ `'AQ'` ∪ `sea_lake_ice` ∪ maritime keywords)): 2,392 open-water events for Looker Studio — see [VIEW job — phenomenons_waters_vw](etl_jobs/view/phenomenon-views.md#phenomenons_waters_vw) |

## Data

Downloaded CSV files from [GEO_Download](../../GEO_Download) are the input. Current sources:

| Source | Plain Table | Records | Load Type |
|--------|-------------|---------|-----------|
| Avalanche_ORG | `geo.avalanche_org` | 4,824 | HISTORICAL |
| EONET | `geo.eonet_event` | 145 | HISTORICAL |
| GDACS | `geo.gdacs_event` | 9,118 | HISTORICAL |
| IBTrACS | `geo.ibtracs_cyclone` | 897 | HISTORICAL |
| NIFC | `geo.nifc_fire` | 233 | HISTORICAL |
| OpenMeteo_Flood | `geo.openmeteo_flood` | 7,326 | HISTORICAL |
| USGS_Earthquake | `geo.usgs_earthquake` | 8,725 | HISTORICAL |
| USGS_Volcano | `geo.usgs_volcano` | 56 | HISTORICAL |
| USGS_NWIS | `geo.usgs_nwis` | 54 | HISTORICAL |

Load type is decided per run — see [ETL jobs — Load type detection](etl_jobs/load-types.md).

## Adding a new table

1. Create a Pydantic model in `model/plain/` (typed) or `model/etl/` (canonical event row).
2. Register it in `model/__init__.py` with `TableFactory.register("<table_name>", <Model>)`.
3. Register an upload script (`factory/upload_script_factory.py`) for the INGESTION job and implement its `_convert()` type casts.
4. Create a corresponding test file under `test/`.
5. Create the table in Supabase — see [Usage — Create tables](usage.md#create-tables-and-run-ingestion).

## Verifying the schema

`python run_pyspark_supabase_test.py` lists every schema and its table count — `geo` should show the registered tables. Full command and expected output: [Usage — Verify Supabase connectivity](usage/maintenance.md#verify-supabase-connectivity).

## Dropping tables and schemas

`run_drop_table.py` drops one table; `run_drop_schema.py` drops an empty schema and refuses one that still contains tables. Commands and expected output: [Usage — Drop a table](usage/maintenance.md#drop-a-table) · [Usage — Drop a schema](usage/maintenance.md#drop-a-schema).

## Cleaning up the latest file_date

Re-running an INCREMENTAL test needs the latest `file_date` removed from the plain table; pass the plain table name. Command and expected output: [Usage — Delete latest file_date data](usage/maintenance.md#delete-latest-file_date-data).
