← [README](../README.md)

# Project Structure

The codebase is organized in layers: `jobs/` decides *what* to do, `factory/` resolves table and script names to implementations, `ingestion/upload/` and `ingestion/raw2plain/` hold script classes with their interfaces (`upload_script.py`, `raw2plain_script.py`), `single_etl/` holds per-dataset PySpark scripts (interface in `single_etl/single_etl_script.py`) run by the `SINGLE_ETL` job — including a `union_etl` script that stacks their output into one `geo.union_etl` table, `views/` holds view-defining PySpark scripts (interface in `views/view_script.py`) run by the `VIEW` job (each defines one `*_vw` view over a `*_etl` table, HISTORICAL only), `model/` defines the Pydantic schemas that drive DDL and Spark types, and `utils/` carries the shared logic.

```
GEO_ETL/
├── config.json                       # App config (Spark, database, logging, data paths, dataset constants)
├── environment.yml                   # Conda environment definition
├── pyproject.toml                    # Ruff, pytest, mypy and coverage configuration
├── tox.ini                           # Tox environments (lint, format, typecheck, py311, warnings)
├── .env.example                      # Template for Supabase credentials (copy to .env, which is not in git)
├── run_etl.py                        # Main CLI entry point for ETL jobs
├── run_drop_table.py                 # Drop a table from Supabase
├── run_drop_schema.py                # Drop a schema from Supabase (if empty)
├── run_delete_last_filedate_data.py  # Delete latest file_date from raw + plain (or ETL) tables
├── run_pyspark_csv_test.py           # Cluster verification script
├── run_pyspark_supabase_test.py      # Supabase connection test script
│
├── ingestion/                        # Ingestion scripts (CSV upload + raw-to-plain transforms)
│   ├── __init__.py
│   ├── upload/                       # Upload scripts (UploadScript interface + script classes)
│   │   ├── __init__.py
│   │   ├── upload_script.py           # UploadScript interface (TABLE_NAME, MODEL, DATASET_FOLDER + upload())
│   │   ├── avalanche_org_upload.py       # Upload Avalanche_ORG CSVs -> Supabase via JDBC
│   │   ├── eonet_event_upload.py         # Upload EONET CSVs -> Supabase via JDBC
│   │   ├── gdacs_event_upload.py         # Upload GDACS CSVs -> Supabase via JDBC
│   │   ├── ibtracs_cyclone_upload.py     # Upload IBTrACS CSVs -> Supabase via JDBC
│   │   ├── nifc_fire_upload.py           # Upload NIFC CSVs -> Supabase via JDBC
│   │   ├── openmeteo_flood_upload.py     # Upload OpenMeteo_Flood CSVs -> Supabase via JDBC
│   │   ├── usgs_earthquake_upload.py     # Upload USGS_Earthquake CSVs -> Supabase via JDBC
│   │   ├── usgs_nwis_upload.py           # Upload USGS_NWIS CSVs -> Supabase via JDBC
│   │   └── usgs_volcano_upload.py        # Upload USGS_Volcano CSVs -> Supabase via JDBC
│   └── raw2plain/                    # Raw-to-plain scripts (Raw2PlainScript interface + script classes)
│       ├── __init__.py
│       ├── raw2plain_script.py        # Raw2PlainScript interface (RAW_TABLE, PLAIN_TABLE + _convert())
│       ├── avalanche_org_raw2plain.py    # Transform Avalanche_ORG raw -> typed data
│       ├── eonet_event_raw2plain.py      # Transform EONET raw -> typed data
│       ├── gdacs_event_raw2plain.py      # Transform GDACS raw -> typed data
│       ├── ibtracs_cyclone_raw2plain.py  # Transform IBTrACS raw -> typed data
│       ├── nifc_fire_raw2plain.py        # Transform NIFC raw -> typed data
│       ├── openmeteo_flood_raw2plain.py  # Transform OpenMeteo_Flood raw -> typed data
│       ├── usgs_earthquake_raw2plain.py  # Transform USGS_Earthquake raw -> typed data
│       ├── usgs_nwis_raw2plain.py        # Transform USGS_NWIS raw -> typed data
│       └── usgs_volcano_raw2plain.py     # Transform USGS_Volcano raw -> typed data
│
├── factory/                          # Registry factories
│   ├── __init__.py                   # Exports all factories (lazy imports)
│   ├── job_factory.py                # JobFactory: job type -> module registry
│   ├── table_factory.py              # TableFactory: table_name -> Pydantic model
│   ├── upload_script_factory.py      # UploadScriptFactory: table_name -> upload script instance
│   ├── raw2plain_script_factory.py   # Raw2PlainScriptFactory: table_name -> raw2plain script instance
│   ├── single_etl_factory.py         # SingleEtlFactory: script name -> script instance
│   └── view_script_factory.py        # ViewScriptFactory: view name -> view script instance
│
├── jobs/                             # Job modules (the "what to do" layer)
│   ├── __init__.py                   # Registers jobs in JobFactory, scripts in the script factories
│   ├── create_table.py              # CREATE TABLE job (DDL from Pydantic models)
│   ├── data_quality.py              # DATA_QUALITY job (per-file_date metrics -> geo.data_quality + overall refresh)
│   ├── ingestion.py                 # INGESTION job (upload + raw2plain via factories)
│   ├── single_etl.py                 # SINGLE_ETL job (runs one script from single_etl/)
│   └── view.py                       # VIEW job (runs one script from views/, defines a *_vw view)
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
├── views/                            # View-defining PySpark scripts (ViewScript interface, VIEW job)
│   ├── __init__.py
│   ├── view_script.py                # ViewScript interface (SOURCE_TABLE, TARGET_VIEW, QUERY + run())
│   ├── metrics_avalanche_vw.py        # geo.union_etl (avalanche rows) -> geo.metrics_avalanche_vw
│   ├── metrics_drought_vw.py          # geo.union_etl (drought rows) -> geo.metrics_drought_vw
│   ├── metrics_earthquake_vw.py       # geo.union_etl (earthquake rows) -> geo.metrics_earthquake_vw
│   ├── metrics_fire_vw.py             # geo.union_etl (fire rows) -> geo.metrics_fire_vw
│   ├── metrics_flood_vw.py            # geo.union_etl (flood rows) -> geo.metrics_flood_vw
│   ├── metrics_iceberg_vw.py          # geo.union_etl (iceberg rows) -> geo.metrics_iceberg_vw
│   ├── metrics_storm_vw.py            # geo.union_etl (storm rows) -> geo.metrics_storm_vw
│   ├── metrics_volcano_vw.py          # geo.union_etl (volcano rows) -> geo.metrics_volcano_vw
│   ├── phenomenons_us_vw.py           # geo.union_etl (US rows) -> geo.phenomenons_us_vw (Looker map view)
│   ├── phenomenons_ca_vw.py           # geo.union_etl (Canada rows) -> geo.phenomenons_ca_vw (Looker map view)
│   ├── phenomenons_cs_amer_vw.py      # geo.union_etl (Americas minus US/CA) -> geo.phenomenons_cs_amer_vw (Looker map view)
│   ├── phenomenons_ru_vw.py           # geo.union_etl (Russia rows) -> geo.phenomenons_ru_vw (Looker map view)
│   ├── phenomenons_europe_vw.py       # geo.union_etl (M49 Europe minus RU) -> geo.phenomenons_europe_vw (Looker map view)
│   ├── phenomenons_asia_vw.py         # geo.union_etl (M49 Asia minus RU) -> geo.phenomenons_asia_vw (Looker map view)
│   ├── phenomenons_africa_vw.py       # geo.union_etl (M49 Africa) -> geo.phenomenons_africa_vw (Looker map view)
│   └── phenomenons_aus_oc_vw.py       # geo.union_etl (M49 Oceania) -> geo.phenomenons_aus_oc_vw (Looker map view)
│
├── model/                            # Data models and configuration
│   ├── enums.py                      # JobType (CREATE, INGESTION, DATA_QUALITY, SINGLE_ETL, VIEW) + LoadType (HISTORICAL, INCREMENTAL)
│   ├── config/
│   │   ├── app_config.py             # AppConfig, SparkConfig, DatabaseConfig, LoggingConfig, DataConfig
│   │   ├── datasets.py               # Per-script dataset constants from config.json (load_dataset + frozen models)
│   │   └── supabase_config.py        # SupabaseConnectionConfig + JDBC helpers
│   ├── raw/
│   │   ├── avalanche_org_raw.py      # AvalancheOrgRaw — all-TEXT raw model
│   │   ├── eonet_event_raw.py        # EonetEventRaw — all-TEXT raw model
│   │   ├── gdacs_event_raw.py        # GdacsEventRaw — all-TEXT raw model
│   │   ├── ibtracs_cyclone_raw.py    # IbtacsCycloneRaw — all-TEXT raw model
│   │   ├── nifc_fire_raw.py          # NifcFireRaw — all-TEXT raw model
│   │   ├── openmeteo_flood_raw.py    # OpenMeteoFloodRaw — all-TEXT raw model
│   │   ├── usgs_earthquake_raw.py    # UsgsEarthquakeRaw — all-TEXT raw model
│   │   ├── usgs_nwis_raw.py          # UsgsNwisRaw — all-TEXT raw model
│   │   └── usgs_volcano_raw.py       # UsgsVolcanoRaw — all-TEXT raw model
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
│       ├── avalanche_org.py          # AvalancheOrg — typed model (int, bool, datetime, date)
│       ├── eonet_event.py            # EonetEvent — typed model (float, int, datetime, date)
│       ├── gdacs_event.py            # GdacsEvent — typed model (float, int, datetime, bool)
│       ├── ibtracs_cyclone.py        # IbtacsCyclone — typed model (float, int, datetime)
│       ├── nifc_fire.py              # NifcFire — typed model (float, str, datetime)
│       ├── openmeteo_flood.py        # OpenMeteoFlood — typed model (float, date)
│       ├── usgs_earthquake.py        # UsgsEarthquake — typed model (float, int, datetime)
│       ├── usgs_nwis.py              # UsgsNwis — typed model (date, float)
│       └── usgs_volcano.py           # UsgsVolcano — typed model (int, float, datetime)
│   └── other/
│       └── data_quality.py           # DataQuality — metric row (str, float, datetime)
│
├── utils/                            # Utility modules
│   ├── ddl_generator.py              # Pydantic -> PostgreSQL DDL + JDBC execution
│   ├── dq_metrics.py                 # Per-file_date data quality rules + metric computation
│   ├── etl_utils.py                  # Shared single-ETL runner (resolve_load_type, run_etl template)
│   ├── jdbc_retry.py                 # Bounded retry for transient pooler connection failures
│   ├── logger.py                     # setup_logger() — file + stdout mirror
│   ├── raw2plain_utils.py            # Shared raw2plain logic (DRY: get_max_file_date, raw2plain template)
│   ├── schema_converter.py           # Pydantic -> PySpark StructType schemas
│   ├── spark_monitor.py              # Stage memory logging via Java SparkListener
│   ├── spark_listener/               # Compiled Java SparkListener classes
│   │   ├── StageMemoryListener.java  # Logs JVM + Spark memory on stage completion
│   │   ├── StageMemoryListener.class
│   │   ├── ListenerHelper.java       # Py4J bridge for listener registration
│   │   ├── ListenerHelper.class
│   │   └── spark-monitor.jar         # Packaged JAR (committed), installed into PySpark's jars/
│   ├── upload_utils.py               # Shared upload logic (DRY: parse_file_date, get_max_file_date, upload template)
│   └── view_utils.py                 # Shared view runner (run_view template, HISTORICAL only)
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
│   ├── raw2plain/                    # Standalone raw2plain transform logs
│   ├── single_etl/                   # SINGLE_ETL job logs
│   ├── view/                         # VIEW job logs
│   ├── pyspark_csv_test/             # CSV test script logs
│   └── pyspark_supabase_test/        # Supabase test script logs
│
├── spark_cluster_image/              # Docker image for Spark cluster
│   ├── Dockerfile                    # Custom Spark image (apache/spark:3.5.5 + JARs)
│   └── jars/
│       ├── hadoop-aws-3.3.4.jar
│       ├── aws-java-sdk-bundle-1.12.262.jar
│       ├── postgresql-42.7.4.jar
│       └── spark-monitor.jar         # Stage memory SparkListener (copied from utils/spark_listener/)
│
└── test/                             # Unit tests (pytest)
    ├── conftest.py                   # Shared test helpers (mock_F_upload, mock_F_raw2plain, mock_app_config_upload, mock_app_config_raw2plain, mock_db_config, mock_spark_upload, mock_spark_raw2plain) + the spark_session fixture
    ├── factory/
    │   ├── test_factory_init.py      # TableFactory registration mechanics
    │   ├── test_job_factory.py
    │   ├── test_script_factories.py  # upload/raw2plain/single_etl script factories (parametrized)
    │   └── test_table_factory.py
    ├── jobs/
    │   ├── test_create_table.py
    │   ├── test_data_quality.py
    │   ├── test_ingestion.py
    │   ├── test_single_etl.py
    │   └── test_view.py                 # VIEW job (script resolution, logging, unknown script, empty-query RuntimeError)
    ├── model/
    │   ├── test_enums.py
    │   ├── test_model_contract.py    # Shared contract for all 28 plain/raw/ETL models
    │   ├── config/
    │   │   ├── test_config.py
    │   │   ├── test_datasets.py
    │   │   └── test_supabase_config.py
    │   ├── etl/
    │   │   └── test_event_etl_models.py   # 10 ETL models incl. UnionEtl (parametrized)
    │   ├── plain/
    │   │   └── test_plain_models.py       # 9 plain models (parametrized)
    │   ├── raw/
    │   │   └── test_raw_models.py         # 9 raw models (parametrized)
    │   └── other/
    │       └── test_data_quality.py
    ├── scripts/
    │   ├── test_run_etl.py
    │   ├── test_run_drop_table.py
    │   ├── test_run_drop_schema.py
    │   └── test_run_delete_last_filedate_data.py
    ├── ingestion/
    │   ├── upload/
    │   │   └── test_upload_scripts.py     # 9 upload scripts (parametrized)
    │   └── raw2plain/
    │       └── test_raw2plain_scripts.py  # 9 raw2plain scripts (parametrized)
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
    │   ├── test_metrics_avalanche_vw.py  # First VIEW script (config wiring, query contract, registry entry)
    │   ├── test_metrics_drought_vw.py    # Drought twin of metrics_avalanche_vw
    │   ├── test_metrics_earthquake_vw.py  # Earthquake twin of metrics_avalanche_vw
    │   ├── test_metrics_fire_vw.py       # Fire twin of metrics_avalanche_vw
    │   ├── test_metrics_flood_vw.py      # Flood twin of metrics_avalanche_vw
    │   ├── test_metrics_iceberg_vw.py    # Iceberg twin of metrics_avalanche_vw
    │   ├── test_metrics_storm_vw.py      # Storm twin of metrics_avalanche_vw
    │   ├── test_metrics_volcano_vw.py    # Volcano twin of metrics_avalanche_vw
    │   ├── test_phenomenons_us_vw.py     # US Looker Studio map view (denormalized)
    │   ├── test_phenomenons_ca_vw.py     # Canada twin (query equality beyond country code)
    │   ├── test_phenomenons_cs_amer_vw.py  # Americas twin (ISO whitelist minus US/CA)
    │   ├── test_phenomenons_ru_vw.py     # Russia twin (denormalized)
    │   ├── test_phenomenons_europe_vw.py  # Europe twin (M49 whitelist minus RU)
    │   ├── test_phenomenons_asia_vw.py   # Asia twin (M49 whitelist minus RU, Europe's complement)
    │   ├── test_phenomenons_africa_vw.py  # Africa twin (M49 whitelist, 54 AU members)
    │   └── test_phenomenons_aus_oc_vw.py  # Australia/Oceania twin (M49 whitelist, four sub-regions)
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
```
