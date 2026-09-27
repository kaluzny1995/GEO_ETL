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
| `test/factory/test_table_factory.py` | TableFactory (register, get, tables, clear) |
| `test/factory/test_upload_script_factory.py` | UploadScriptFactory (register, get, tables, clear) |
| `test/factory/test_raw2plain_script_factory.py` | Raw2PlainScriptFactory (register, get, tables, clear) |
| `test/jobs/test_create_table.py` | CREATE TABLE job logic, schema creation, DDL execution |
| `test/jobs/test_data_quality.py` | DATA_QUALITY job (HISTORICAL + INCREMENTAL load types, stale-metric purge, replace, no-new-data, logging, unknown table) |
| `test/jobs/test_ingestion.py` | INGESTION job (upload + raw2plain ordering, config passing, logger passing, logging) |
| `test/model/test_enums.py` | JobType enum (values, membership, from_value, invalid value) |
| `test/model/config/test_config.py` | AppConfig, SparkConfig, DatabaseConfig, LoggingConfig, DataConfig models |
| `test/model/config/test_supabase_config.py` | SupabaseConnectionConfig, JDBC URL/properties |
| `test/model/raw/test_avalanche_org_raw.py` | AvalancheOrgRaw model validation + Spark schema |
| `test/model/raw/test_eonet_event_raw.py` | EonetEventRaw model validation + Spark schema |
| `test/model/raw/test_gdacs_event_raw.py` | GdacsEventRaw model validation + Spark schema |
| `test/model/raw/test_ibtracs_cyclone_raw.py` | IbtacsCycloneRaw model validation + Spark schema |
| `test/model/raw/test_nifc_fire_raw.py` | NifcFireRaw model validation + Spark schema |
| `test/model/raw/test_openmeteo_flood_raw.py` | OpenMeteoFloodRaw model validation + Spark schema |
| `test/model/raw/test_usgs_earthquake_raw.py` | UsgsEarthquakeRaw model validation + Spark schema |
| `test/model/raw/test_usgs_nwis_raw.py` | UsgsNwisRaw model validation + Spark schema |
| `test/model/raw/test_usgs_volcano_raw.py` | UsgsVolcanoRaw model validation + Spark schema |
| `test/model/plain/test_avalanche_org.py` | AvalancheOrg model validation + Spark schema + DDL types |
| `test/model/plain/test_eonet_event.py` | EonetEvent model validation + Spark schema + DDL types |
| `test/model/plain/test_gdacs_event.py` | GdacsEvent model validation + Spark schema + DDL types |
| `test/model/plain/test_ibtracs_cyclone.py` | IbtacsCyclone model validation + Spark schema + DDL types |
| `test/model/plain/test_nifc_fire.py` | NifcFire model validation + Spark schema + DDL types |
| `test/model/plain/test_openmeteo_flood.py` | OpenMeteoFlood model validation + Spark schema + DDL types |
| `test/model/plain/test_usgs_earthquake.py` | UsgsEarthquake model validation + Spark schema + DDL types |
| `test/model/plain/test_usgs_nwis.py` | UsgsNwis model validation + Spark schema + DDL types |
| `test/model/plain/test_usgs_volcano.py` | UsgsVolcano model validation + Spark schema + DDL types |
| `test/model/other/test_data_quality.py` | DataQuality model validation + Spark schema + DDL types + registry |
| `test/ingestion/upload/test_avalanche_org_upload.py` | Upload script (helpers + upload: historical, incremental, no files, logging) |
| `test/ingestion/upload/test_eonet_event_upload.py` | EONET upload script (helpers + upload: historical, incremental, no files, logging) |
| `test/ingestion/upload/test_gdacs_event_upload.py` | GDACS upload script (helpers + upload: historical, incremental, no files, logging) |
| `test/ingestion/upload/test_ibtracs_cyclone_upload.py` | IBTrACS upload script (helpers + upload: historical, incremental, no files, logging) |
| `test/ingestion/upload/test_nifc_fire_upload.py` | NIFC upload script (helpers + upload: historical, incremental, no files, logging) |
| `test/ingestion/upload/test_openmeteo_flood_upload.py` | OpenMeteo upload script (helpers + upload: historical, incremental, no files, logging) |
| `test/ingestion/upload/test_usgs_earthquake_upload.py` | USGS Earthquake upload script (helpers + upload: historical, incremental, no files, logging) |
| `test/ingestion/upload/test_usgs_nwis_upload.py` | USGS NWIS upload script (helpers + upload: historical, incremental, no files, logging) |
| `test/ingestion/upload/test_usgs_volcano_upload.py` | USGS Volcano upload script (helpers + upload: historical, incremental, no files, logging) |
| `test/ingestion/raw2plain/test_avalanche_org_raw2plain.py` | Raw2Plain transform (historical, incremental, no records, type conversion) |
| `test/ingestion/raw2plain/test_eonet_event_raw2plain.py` | EONET Raw2Plain transform (historical, incremental, no records, type conversion) |
| `test/ingestion/raw2plain/test_gdacs_event_raw2plain.py` | GDACS Raw2Plain transform (historical, incremental, no records, type conversion) |
| `test/ingestion/raw2plain/test_ibtracs_cyclone_raw2plain.py` | IBTrACS Raw2Plain transform (historical, incremental, no records, type conversion) |
| `test/ingestion/raw2plain/test_nifc_fire_raw2plain.py` | NIFC Raw2Plain transform (historical, incremental, no records, type conversion) |
| `test/ingestion/raw2plain/test_openmeteo_flood_raw2plain.py` | OpenMeteo Raw2Plain transform (historical, incremental, no records, type conversion) |
| `test/ingestion/raw2plain/test_usgs_earthquake_raw2plain.py` | USGS Earthquake Raw2Plain transform (historical, incremental, no records, type conversion) |
| `test/ingestion/raw2plain/test_usgs_nwis_raw2plain.py` | USGS NWIS Raw2Plain transform (historical, incremental, no records, type conversion) |
| `test/ingestion/raw2plain/test_usgs_volcano_raw2plain.py` | USGS Volcano Raw2Plain transform (historical, incremental, no records, type conversion) |
| `test/scripts/test_run_etl.py` | run_etl.py (CLI dispatch, Spark session lifecycle, logger passing, error handling) |
| `test/scripts/test_run_drop_table.py` | run_drop_table.py CLI script |
| `test/scripts/test_run_drop_schema.py` | run_drop_schema.py CLI script |
| `test/scripts/test_run_delete_last_filedate_data.py` | run_delete_last_filedate_data.py (delete latest file_date, table derivation, skip on empty) |
| `test/util/test_ddl_generator.py` | DDL generation, create/execute schema, drop table, drop schema, schema table check |
| `test/util/test_dq_metrics.py` | Rules, metric computation, unpivoting — run on a real local Spark session |
| `test/util/test_jdbc_retry.py` | Connection-error detection, bounded retries, delay and warning per attempt |
| `test/util/test_schema_converter.py` | Pydantic -> PySpark StructType conversion |
| `test/util/test_spark_monitor.py` | Stage memory logger registration (Java bridge, directory creation, listener setup) |
| `test/util/test_logger.py` | Logger setup, duplicate handlers, nested directories |
