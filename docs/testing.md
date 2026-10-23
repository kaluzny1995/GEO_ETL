← [README](../README.md)

# Testing

Tests are unit tests with external dependencies (Spark cluster, Supabase) mocked:

```bash
conda activate geo_etl_311
pytest test/ -v
```

The only exception is the `spark_session` fixture in `test/conftest.py` — a session-scoped local `SparkSession` (`local[2]`, UI disabled) used by `test/util/test_dq_metrics.py` (real DataFrame execution) and by the INCREMENTAL case of `test/jobs/test_data_quality.py` (building the `file_date` filter needs a live context). The fixture skips the test when no JVM is available, so the suite still runs on a machine without Java. Everything else stays fully mocked.

The same suite also runs in an isolated tox environment (`tox -e py311`) — see [Code quality](development.md#tox-environments) for the full check matrix.

## Test structure

| Test Suite | What It Tests |
|------------|---------------|
| `test/factory/test_job_factory.py` | JobFactory (register, create, clear) |
| `test/factory/test_factory_init.py` | Unknown-attribute access raises AttributeError |
| `test/factory/test_script_factories.py` | UploadScriptFactory, Raw2PlainScriptFactory, SingleEtlFactory, ViewScriptFactory parametrized (register, get, tables/scripts, clear) |
| `test/factory/test_table_factory.py` | TableFactory registry mechanics (register/get, unknown table raises `ValueError`, clear + restore, overwrite, `tables()` listing) |
| `test/jobs/test_create_table.py` | CREATE TABLE job logic, schema creation, DDL execution |
| `test/jobs/test_data_quality.py` | DATA_QUALITY job (HISTORICAL + INCREMENTAL load types, `overall` refresh, `--overall` mode, stale-metric purge, replace, no-new-data, logging, unknown table) |
| `test/jobs/test_ingestion.py` | INGESTION job (upload + raw2plain ordering, config passing, logger passing, logging) |
| `test/jobs/test_single_etl.py` | SINGLE_ETL job (script resolution, load type and logger passing, unknown script, raises `RuntimeError` when the run loaded no records) |
| `test/jobs/test_view.py` | VIEW job (script resolution, logger passing, no `load_type` kwarg, config loading, unknown script, raises `RuntimeError` when the query returned no rows) |
| `test/model/test_enums.py` | JobType and LoadType enums (values, membership, from_value, invalid value) |
| `test/model/config/test_config.py` | AppConfig, SparkConfig, DatabaseConfig, LoggingConfig, DataConfig models |
| `test/model/config/test_datasets.py` | Dataset constants: `load_dataset()` keyed by script file stem, frozen/extra-forbid models (incl. `ViewDatasetConfig` for the `view` section), project config sections (incl. the union script's `source_tables` and the registered view entries) |
| `test/model/config/test_supabase_config.py` | SupabaseConnectionConfig, JDBC URL/properties |
| `test/model/test_model_contract.py` | Shared contract for all 28 canonical models (plain + raw + ETL): full-row kwargs, optional fields default to None, Spark schema field count/names/nullable/type, missing `file_date` raises |
| `test/model/raw/test_raw_models.py` | All nine raw models parametrized (all-fields-accepted, optional present, missing required field, all-string Spark schema) + NIFC CSV header order |
| `test/model/plain/test_plain_models.py` | All nine plain models parametrized (missing required/key field, DDL fragments, DDL create table, factory registry) + IBTrACS field-name membership |
| `test/model/other/test_data_quality.py` | DataQuality model validation + Spark schema + DDL types + registry |
| `test/model/etl/test_event_etl_models.py` | The nine source ETL models + `UnionEtl` parametrized (missing required fields, empty model raises, canonical field order, field names, non-nullable required fields, DDL header/column types, registry) |
| `test/ingestion/upload/test_upload_scripts.py` | All nine upload scripts parametrized (helpers + upload: historical, incremental, no files, logging, parse/max-file-date) |
| `test/ingestion/raw2plain/test_raw2plain_scripts.py` | All nine Raw2Plain transforms parametrized (historical, incremental, no records, type conversion, max-file-date helper incl. the union's per-`source_name` scoping) |
| `test/scripts/test_run_etl.py` | run_etl.py (CLI dispatch incl. `VIEW`, `--overall` / `--load-type` incl. the VIEW rejection, Spark session lifecycle, logger passing, error handling) |
| `test/scripts/test_run_drop_table.py` | run_drop_table.py CLI script |
| `test/scripts/test_run_drop_schema.py` | run_drop_schema.py CLI script |
| `test/scripts/test_run_delete_last_filedate_data.py` | run_delete_last_filedate_data.py (delete latest file_date from raw + plain or ETL-only, table derivation, skip on empty, data_quality rejection) |
| `test/single_etl/test_shared_etl_behavior.py` | Run behaviour shared by all ten SINGLE_ETL scripts (the nine source scripts + `union_etl`) parametrized (auto detection historical/incremental, forced historical, forced incremental on empty target, `loaded=False` for no-new-rows / empty conversion, source table) + the union's per-source INCREMENTAL thresholds (keyed by each table's `source_name`) |
| `test/single_etl/test_union_etl.py` | union script (nine configured source tables, identity `_convert`, target from config, registry entry) |
| `test/views/test_metrics_avalanche_vw.py` | metrics_avalanche_vw view script (source/target/query from config, `_vw` suffix, avalanche filter + numeric `other_metrics` explosion + `file_date` cast in the query, registry entry) |
| `test/views/test_metrics_drought_vw.py` | metrics_drought_vw view script (same contract as the avalanche view, drought filter) |
| `test/views/test_metrics_earthquake_vw.py` | metrics_earthquake_vw view script (same contract as the avalanche view, earthquake filter + rare magnitude-scale exclusion) |
| `test/views/test_metrics_fire_vw.py` | metrics_fire_vw view script (same contract as the avalanche view, fire filter) |
| `test/views/test_metrics_flood_vw.py` | metrics_flood_vw view script (same contract as the avalanche view, flood filter) |
| `test/single_etl/test_avalanche_org_etl.py` | Avalanche SINGLE_ETL script (column mapping, source coordinate pass-through, forecast-window collapse) |
| `test/single_etl/test_eonet_event_etl.py` | EONET SINGLE_ETL script (category/place/country mapping, stamped geocode pass-through with description-state precedence) |
| `test/single_etl/test_gdacs_event_etl.py` | GDACS SINGLE_ETL script (category/metric/country mapping, stamped geocode pass-through with alpha-3 precedence) |
| `test/single_etl/test_ibtracs_cyclone_etl.py` | IBTrACS SINGLE_ETL script (status/nature/place/metric/danger mapping, stamped geocode pass-through with ZZ fallback, latest position) |
| `test/single_etl/test_nifc_fire_etl.py` | NIFC SINGLE_ETL script (fire type/unit/percent mapping, unit-name pass-through, stamped geocode fallback for blank source state, NULL event_end) |
| `test/single_etl/test_openmeteo_flood_etl.py` | OpenMeteo SINGLE_ETL script (country/state/place mapping, stamped geocode pass-through with country-name precedence, discharge metric, threshold-reference join with missing/invalid file handling, danger ladder, ensemble forecast metrics) |
| `test/single_etl/test_usgs_earthquake_etl.py` | USGS SINGLE_ETL script (place-suffix country parse with stamped geocode fallbacks, net-based state, event name/description, depth/station metrics) |
| `test/single_etl/test_usgs_nwis_etl.py` | USGS NWIS SINGLE_ETL script (site-name event/place parse, qualifier expansion, stamped geocode state, discharge metric, flood-category danger ladder, flood-category reference join with missing/absent file handling, other_metrics peak + NWS thresholds) |
| `test/single_etl/test_usgs_volcano_etl.py` | USGS Volcano SINGLE_ETL script (VEI metric with unit and `VEI <n>` danger rating, other-metrics omissions incl. alert level and NCEI casualties/damage, subcategory/place/state fallbacks with stamped geocode state) |
| `test/util/test_ddl_generator.py` | DDL generation, create/execute schema, drop table, drop schema, schema table check |
| `test/util/test_dq_metrics.py` | Rules, rate/variance metric computation, unpivoting — run on a real local Spark session |
| `test/util/test_etl_utils.py` | Shared SINGLE_ETL runner `utils/etl_utils.py`: load-type resolution (auto → historical/incremental, forced type wins) |
| `test/util/test_jdbc_retry.py` | Connection-error detection, bounded retries, delay and warning per attempt |
| `test/util/test_schema_converter.py` | Pydantic -> PySpark StructType conversion |
| `test/util/test_script_bases.py` | Script interface contracts: `SingleEtlScript` ABC — abstract `_convert()`, `run()` delegates tables, convert, load type and logger to `run_etl`; `Raw2PlainScript` ABC — `raw2plain()` delegates tables, convert and logger to the shared runner; `UploadScript` ABC — abstract config attributes, `upload()` delegates table, model, folder and logger to the shared runner; `ViewScript` ABC — `run()` delegates table, view name, query and logger to `run_view` |
| `test/util/test_spark_monitor.py` | Stage memory logger registration (Java bridge, directory creation, listener setup) |
| `test/util/test_view_utils.py` | Shared VIEW runner `utils/view_utils.py`: query executed as a JDBC subquery, `CREATE OR REPLACE VIEW` DDL (trailing `;` stripped from the query), `loaded=False` without a definition on an empty result, HISTORICAL-only load type, logging |
| `test/util/test_logger.py` | Logger setup, duplicate handlers (file + stdout mirror), nested directories |
