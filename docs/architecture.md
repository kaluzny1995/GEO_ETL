← [README](../README.md)

# Project Structure

The codebase is organized in layers: `jobs/` decides *what* to do, `factory/` resolves table and script names to implementations, `ingestion/upload/` holds the CSV upload/convert script classes (interface in `upload_script.py`), `single_etl/` holds per-dataset PySpark scripts (interface in `single_etl/single_etl_script.py`) run by the `SINGLE_ETL` job — including a `union_etl` script that stacks their output into one `geo.union_etl` table, `views/` holds view-defining PySpark scripts (interface in `views/view_script.py`) run by the `VIEW` job (each defines one `*_vw` view over a `*_etl` table, HISTORICAL only), `ml/` holds the PySpark MLlib model training/selection/persistence logic used by the `PREDICTION` job, `model/` defines the Pydantic schemas that drive DDL and Spark types, and `utils/` carries the shared logic.

```
GEO_ETL/
├── config.json                       # App config (Spark, database, logging, data paths, dataset constants)
├── environment.yml                   # Conda environment definition
├── pyproject.toml                    # Ruff, pytest, mypy and coverage configuration
├── tox.ini                           # Tox environments (lint, format, typecheck, py311, warnings)
├── .env.example                      # Template for Supabase credentials (the real .env is gitignored; normally written by GEO_E2E's setup.sh — see docs/configuration/supabase-credentials.md)
├── run_etl.py                        # Main CLI entry point for ETL jobs
├── run_drop_table.py                 # Drop a table from Supabase
├── run_drop_schema.py                # Drop a schema from Supabase (if empty)
├── run_delete_last_filedate_data.py  # Delete latest file_date from a plain (or ETL) table
├── run_pyspark_csv_test.py           # Cluster verification script
├── run_pyspark_supabase_test.py      # Supabase connection test script
│
├── ingestion/                        # Ingestion scripts (CSV → typed plain table)
│   ├── __init__.py
│   ├── upload/                       # Upload scripts (UploadScript interface + script classes)
│   │   ├── __init__.py
│   │   ├── upload_script.py           # UploadScript interface (TABLE_NAME, DATASET_FOLDER + _convert() + upload())
│   │   ├── avalanche_org_upload.py       # Convert Avalanche_ORG CSVs -> typed Supabase table
│   │   ├── eonet_event_upload.py         # Convert EONET CSVs -> typed Supabase table
│   │   ├── gdacs_event_upload.py         # Convert GDACS CSVs -> typed Supabase table
│   │   ├── ibtracs_cyclone_upload.py     # Convert IBTrACS CSVs -> typed Supabase table
│   │   ├── nifc_fire_upload.py           # Convert NIFC CSVs -> typed Supabase table
│   │   ├── openmeteo_flood_upload.py     # Convert OpenMeteo_Flood CSVs -> typed Supabase table
│   │   ├── usgs_earthquake_upload.py     # Convert USGS_Earthquake CSVs -> typed Supabase table
│   │   ├── usgs_nwis_upload.py           # Convert USGS_NWIS CSVs -> typed Supabase table
│   │   └── usgs_volcano_upload.py        # Convert USGS_Volcano CSVs -> typed Supabase table
├── factory/                          # Registry factories
│   ├── __init__.py                   # Exports all factories (lazy imports)
│   ├── job_factory.py                # JobFactory: job type -> module registry
│   ├── table_factory.py              # TableFactory: table_name -> Pydantic model
│   ├── upload_script_factory.py      # UploadScriptFactory: table_name -> upload script instance
│   ├── single_etl_factory.py         # SingleEtlFactory: script name -> script instance
│   └── view_script_factory.py        # ViewScriptFactory: view name -> view script instance
│
├── jobs/                             # Job modules (the "what to do" layer)
│   ├── __init__.py                   # Registers jobs in JobFactory, scripts in the script factories
│   ├── create_table.py              # CREATE TABLE job (DDL from Pydantic models)
│   ├── data_quality.py              # DATA_QUALITY job (per-file_date metrics -> geo.data_quality + overall refresh)
│   ├── data_source_file.py          # DATA_SOURCE_FILE job (CSV inventory -> geo.data_source_files)
│   ├── ingestion.py                 # INGESTION job (CSV → typed plain table via the upload factory)
│   ├── single_etl.py                 # SINGLE_ETL job (runs one script from single_etl/)
│   ├── view.py                       # VIEW job (runs one script from views/, defines a *_vw view)
│   └── prediction.py                 # PREDICTION job (MLlib models -> geo.prediction_etl)
│
├── single_etl/                       # Standalone PySpark ETL scripts (SingleEtlScript interface, SINGLE_ETL job)
│   ├── __init__.py
│   ├── single_etl_script.py           # SingleEtlScript interface (SOURCE_TABLE, TARGET_TABLE + _convert())
│   ├── avalanche_org_etl.py           # geo.avalanche_org -> geo.avalanche_org_etl (canonical event structure)
│   ├── eonet_event_etl.py             # geo.eonet_event -> geo.eonet_event_etl (canonical event structure)
│   ├── gdacs_event_etl.py             # geo.gdacs_event -> geo.gdacs_event_etl (canonical event structure)
│   ├── ibtracs_cyclone_etl.py         # geo.ibtracs_cyclone -> geo.ibtracs_cyclone_etl (canonical event structure)
│   ├── nifc_fire_etl.py               # geo.nifc_fire -> geo.nifc_fire_etl (canonical event structure)
│   ├── openmeteo_flood_etl.py         # geo.openmeteo_flood -> geo.openmeteo_flood_etl (canonical event structure)
│   ├── usgs_earthquake_etl.py         # geo.usgs_earthquake -> geo.usgs_earthquake_etl (canonical event structure)
│   ├── usgs_nwis_etl.py               # geo.usgs_nwis -> geo.usgs_nwis_etl (canonical event structure)
│   ├── usgs_volcano_etl.py            # geo.usgs_volcano -> geo.usgs_volcano_etl (canonical event structure)
│   └── union_etl.py                   # every *_etl table -> geo.union_etl (stacks the nine canonical tables)
│
├── ml/                               # PySpark MLlib prediction logic (PREDICTION job)
│   ├── __init__.py
│   └── predictor.py                  # Per-metric training, candidate selection, persistence, reuse/retraining
│
├── ml_models/                        # Persisted Spark MLlib models + metadata (git-ignored, bind-mounted in Docker)
│
├── views/                            # View-defining PySpark scripts (ViewScript interface, VIEW job)
│   ├── __init__.py
│   ├── view_script.py                # ViewScript interface (SOURCE_TABLE, TARGET_VIEW, QUERY + run())
│   ├── metrics_observed_vw.py         # geo.union_etl -> geo.metrics_observed_vw (shared observed metrics)
│   ├── metrics_avalanche_vw.py        # geo.metrics_observed_vw + geo.prediction_etl -> geo.metrics_avalanche_vw
│   ├── metrics_drought_vw.py          # observed + predicted drought rows -> geo.metrics_drought_vw
│   ├── metrics_earthquake_vw.py       # observed + predicted earthquake rows -> geo.metrics_earthquake_vw
│   ├── metrics_fire_vw.py             # observed + predicted fire rows -> geo.metrics_fire_vw
│   ├── metrics_flood_vw.py            # observed + predicted flood rows -> geo.metrics_flood_vw
│   ├── metrics_iceberg_vw.py          # observed + predicted iceberg rows -> geo.metrics_iceberg_vw
│   ├── metrics_storm_vw.py            # observed + predicted storm rows -> geo.metrics_storm_vw
│   ├── metrics_volcano_vw.py          # observed + predicted volcano rows -> geo.metrics_volcano_vw
│   ├── phenomenons_us_vw.py           # geo.union_etl (US rows) -> geo.phenomenons_us_vw (Looker map view)
│   ├── phenomenons_ca_vw.py           # geo.union_etl (Canada rows) -> geo.phenomenons_ca_vw (Looker map view)
│   ├── phenomenons_cs_amer_vw.py      # geo.union_etl (Americas minus US/CA) -> geo.phenomenons_cs_amer_vw (Looker map view)
│   ├── phenomenons_ru_vw.py           # geo.union_etl (Russia rows) -> geo.phenomenons_ru_vw (Looker map view)
│   ├── phenomenons_europe_vw.py       # geo.union_etl (M49 Europe minus RU) -> geo.phenomenons_europe_vw (Looker map view)
│   ├── phenomenons_asia_vw.py         # geo.union_etl (M49 Asia minus RU) -> geo.phenomenons_asia_vw (Looker map view)
│   ├── phenomenons_africa_vw.py       # geo.union_etl (M49 Africa) -> geo.phenomenons_africa_vw (Looker map view)
│   ├── phenomenons_aus_oc_vw.py       # geo.union_etl (M49 Oceania) -> geo.phenomenons_aus_oc_vw (Looker map view)
│   └── phenomenons_waters_vw.py       # geo.union_etl (open-water heuristic) -> geo.phenomenons_waters_vw (Looker map view)
│
├── model/                            # Data models and configuration
│   ├── enums.py                      # JobType (CREATE, INGESTION, DATA_QUALITY, DATA_SOURCE_FILE, SINGLE_ETL, VIEW, PREDICTION) + LoadType (HISTORICAL, INCREMENTAL)
│   ├── config/
│   │   ├── app_config.py             # AppConfig, SparkConfig, DatabaseConfig, LoggingConfig, DataConfig
│   │   ├── datasets.py               # Per-script dataset constants from config.json (load_dataset + frozen models; PredictionConfig)
│   │   └── supabase_config.py        # SupabaseConnectionConfig + JDBC helpers
│   ├── etl/
│   │   ├── event_etl.py               # EventEtl — canonical event structure shared by all *_etl tables
│   │   ├── avalanche_org_etl.py       # AvalancheOrgEtl — geo.avalanche_org_etl model
│   │   ├── eonet_event_etl.py         # EonetEventEtl — geo.eonet_event_etl model
│   │   ├── gdacs_event_etl.py         # GdacsEventEtl — geo.gdacs_event_etl model
│   │   ├── ibtracs_cyclone_etl.py     # IbtracsCycloneEtl — geo.ibtracs_cyclone_etl model
│   │   ├── nifc_fire_etl.py           # NifcFireEtl — geo.nifc_fire_etl model
│   │   ├── openmeteo_flood_etl.py     # OpenMeteoFloodEtl — geo.openmeteo_flood_etl model
│   │   ├── usgs_earthquake_etl.py     # UsgsEarthquakeEtl — geo.usgs_earthquake_etl model
│   │   ├── usgs_nwis_etl.py           # UsgsNwisEtl — geo.usgs_nwis_etl model
│   │   ├── usgs_volcano_etl.py        # UsgsVolcanoEtl — geo.usgs_volcano_etl model
│   │   └── union_etl.py               # UnionEtl — geo.union_etl model (every *_etl table stacked)
│   ├── plain/
│   │   ├── avalanche_org.py          # AvalancheOrg — typed model (int, bool, datetime, date)
│   │   ├── eonet_event.py            # EonetEvent — typed model (float, int, datetime, date)
│   │   ├── gdacs_event.py            # GdacsEvent — typed model (float, int, datetime, bool)
│   │   ├── ibtracs_cyclone.py        # IbtacsCyclone — typed model (float, int, datetime)
│   │   ├── nifc_fire.py              # NifcFire — typed model (float, str, datetime)
│   │   ├── openmeteo_flood.py        # OpenMeteoFlood — typed model (float, date)
│   │   ├── usgs_earthquake.py        # UsgsEarthquake — typed model (float, int, datetime)
│   │   ├── usgs_nwis.py              # UsgsNwis — typed model (date, float)
│   │   └── usgs_volcano.py           # UsgsVolcano — typed model (int, float, datetime)
│   └── other/
│       ├── data_quality.py           # DataQuality — metric row (str, float, datetime)
│       ├── data_source_file.py       # DataSourceFile / DataSourceSource — published CSV and source rows
│       └── prediction_etl.py         # PredictionEtl — predicted metric/date row + model metadata
│
├── utils/                            # Utility modules
│   ├── data_source_files.py          # Publishes source CSV metadata to geo.data_source_files
│   ├── ddl_generator.py              # Pydantic -> PostgreSQL DDL + JDBC execution
│   ├── dq_metrics.py                 # Per-file_date data quality rules + metric computation
│   ├── etl_utils.py                  # Shared single-ETL runner (resolve_load_type, run_etl template)
│   ├── jdbc_retry.py                 # Bounded retry for transient pooler connection failures
│   ├── logger.py                     # setup_logger() — file + stdout mirror
│   ├── schema_converter.py           # Pydantic -> PySpark StructType schemas
│   ├── spark_monitor.py              # Stage memory logging via Java SparkListener
│   ├── spark_listener/               # Compiled Java SparkListener classes
│   │   ├── StageMemoryListener.java  # Logs JVM + Spark memory on stage completion
│   │   ├── StageMemoryListener.class
│   │   ├── ListenerHelper.java       # Py4J bridge for listener registration
│   │   ├── ListenerHelper.class
│   │   └── spark-monitor.jar         # Packaged JAR (committed), installed into PySpark's jars/
│   ├── upload_utils.py               # Shared CSV ingestion logic (parse_file_date, get_max_file_date, upload template)
│   └── view_utils.py                 # Shared view runner (run_view template, HISTORICAL only)
│   └── column_helpers.py             # Shared Spark column expressions (literal_map, state_province)
│
├── docs/                             # Documentation (this file included)
│
├── logs/                             # Runtime log output (gitignored)
│   ├── ingestion/                    # INGESTION job logs
│   ├── create/                       # CREATE TABLE job logs
│   ├── data_quality/                 # DATA_QUALITY job logs
│   ├── drop_table/                   # Drop table job logs
│   ├── drop_schema/                  # Drop schema job logs
│   ├── delete_last_filedate/         # Delete last file_date job logs
│   ├── memory/                       # Per-stage memory consumption logs
│   ├── upload/                       # Standalone upload job logs
│   ├── single_etl/                   # SINGLE_ETL job logs
│   ├── view/                         # VIEW job logs
│   ├── prediction/                   # PREDICTION job logs
│   ├── pyspark_csv_test/             # CSV test script logs
│   └── pyspark_supabase_test/        # Supabase test script logs
│
└── test/                             # Unit tests (pytest)
    ├── conftest.py                   # Shared test helpers (mock_F_upload, mock_app_config_upload, mock_db_config, mock_spark_upload) + the spark_session fixture
    ├── factory/
    │   ├── test_factory_init.py      # TableFactory registration mechanics
    │   ├── test_job_factory.py
    │   ├── test_script_factories.py  # upload/single_etl/view script factories (parametrized)
    │   └── test_table_factory.py
    ├── jobs/
    │   ├── test_create_table.py
    │   ├── test_data_quality.py
    │   ├── test_ingestion.py
    │   ├── test_single_etl.py
    │   ├── test_view.py                 # VIEW job (script resolution, logging, unknown script, empty-query RuntimeError)
    │   └── test_prediction.py           # PREDICTION job (date ranges, filtering, table-name guard)
    ├── ml/
    │   └── test_predictor.py            # Model training, selection, persistence, reuse and retraining
    ├── model/
    │   ├── test_enums.py
    │   ├── test_model_contract.py    # Shared contract for all 19 plain/ETL models
    │   ├── config/
    │   │   ├── test_config.py
    │   │   ├── test_datasets.py
    │   │   └── test_supabase_config.py
    │   ├── etl/
    │   │   └── test_event_etl_models.py   # 10 ETL models incl. UnionEtl (parametrized)
    │   ├── plain/
    │   │   └── test_plain_models.py       # 9 plain models (parametrized)
    │   └── other/
    │       ├── test_data_quality.py
    │       ├── test_data_source_file.py
    │       └── test_prediction_etl.py
    ├── scripts/
    │   ├── test_run_etl.py
    │   ├── test_run_drop_table.py
    │   ├── test_run_drop_schema.py
    │   └── test_run_delete_last_filedate_data.py
    ├── ingestion/
    │   └── upload/
    │       └── test_upload_scripts.py     # 9 upload scripts (parametrized)
    ├── single_etl/
    │   ├── test_shared_etl_behavior.py    # Run() behavior shared by all 10 scripts
    │   ├── test_avalanche_org_etl.py
    │   ├── test_eonet_event_etl.py
    │   ├── test_gdacs_event_etl.py
    │   ├── test_ibtracs_cyclone_etl.py
    │   ├── test_nifc_fire_etl.py
    │   ├── test_openmeteo_flood_etl.py
    │   ├── test_usgs_earthquake_etl.py
    │   ├── test_usgs_nwis_etl.py
    │   ├── test_usgs_volcano_etl.py
    │   └── test_union_etl.py
    ├── views/
    │   ├── __init__.py
    │   ├── test_metrics_observed_vw.py   # Shared observed-metric view over geo.union_etl
    │   ├── test_metrics_views.py         # All eight metrics views, parametrized by category
    │   ├── test_phenomenons_us_vw.py     # US Looker Studio map view (denormalized)
    │   ├── test_phenomenons_ca_vw.py     # Canada twin (query equality beyond country code)
    │   ├── test_phenomenons_cs_amer_vw.py  # Americas twin (ISO whitelist minus US/CA)
    │   ├── test_phenomenons_ru_vw.py     # Russia twin (denormalized)
    │   ├── test_phenomenons_europe_vw.py  # Europe twin (M49 whitelist minus RU)
    │   ├── test_phenomenons_asia_vw.py   # Asia twin (M49 whitelist minus RU, Europe's complement)
    │   ├── test_phenomenons_africa_vw.py  # Africa twin (M49 whitelist, 54 AU members)
    │   ├── test_phenomenons_aus_oc_vw.py  # Australia/Oceania twin (M49 whitelist, four sub-regions)
    │   └── test_phenomenons_waters_vw.py  # ocean/sea twin (ZZ + sea_lake_ice + keywords)
    └── util/
        ├── test_ddl_generator.py
        ├── test_dq_metrics.py
        ├── test_etl_utils.py
        ├── test_jdbc_retry.py
        ├── test_logger.py
        ├── test_schema_converter.py
        ├── test_script_bases.py
        ├── test_spark_monitor.py
        └── test_view_utils.py            # Shared VIEW runner (subquery read, CREATE OR REPLACE VIEW, empty result)
│       └── test_column_helpers.py        # Shared column helpers (literal_map, state_province)
```

The Spark image lives in [GEO_E2E](../../GEO_E2E) under `docker/geo_airflow_spark/` (`Dockerfile` + JARs) and is built by its `docker-compose.yaml` as `geo_airflow_spark` — see [Spark cluster — Custom Spark image](spark-cluster.md#custom-spark-image).
