← [README](../README.md)

# Project Structure

The codebase is organized in layers: `jobs/` decides *what* to do, `factory/` resolves table and script names to implementations, `ingestion/upload/` and `ingestion/raw2plain/` hold per-table thin wrappers, `single_etl/` holds standalone per-dataset PySpark scripts run by the `SINGLE_ETL` job, `model/` defines the Pydantic schemas that drive DDL and Spark types, and `utils/` carries the shared logic.

```
GEO_ETL/
├── config.json                       # App config (Spark, database, logging, data paths)
├── environment.yml                   # Conda environment definition
├── supabase_config.json              # Supabase credentials (not in git)
├── run_etl.py                        # Main CLI entry point for ETL jobs
├── run_drop_table.py                 # Drop a table from Supabase
├── run_drop_schema.py                # Drop a schema from Supabase (if empty)
├── run_delete_last_filedate_data.py  # Delete latest file_date from raw + plain tables
├── run_pyspark_csv_test.py           # Cluster verification script
├── run_pyspark_supabase_test.py      # Supabase connection test script
│
├── ingestion/                        # Ingestion scripts (CSV upload + raw-to-plain transforms)
│   ├── __init__.py
│   ├── upload/                       # Data upload scripts (thin wrappers around utils/upload_utils.py)
│   │   ├── __init__.py
│   │   ├── avalanche_org_upload.py       # Upload Avalanche_ORG CSVs -> Supabase via JDBC
│   │   ├── eonet_event_upload.py         # Upload EONET CSVs -> Supabase via JDBC
│   │   ├── gdacs_event_upload.py         # Upload GDACS CSVs -> Supabase via JDBC
│   │   ├── ibtracs_cyclone_upload.py     # Upload IBTrACS CSVs -> Supabase via JDBC
│   │   ├── nifc_fire_upload.py           # Upload NIFC CSVs -> Supabase via JDBC
│   │   ├── openmeteo_flood_upload.py     # Upload OpenMeteo_Flood CSVs -> Supabase via JDBC
│   │   ├── usgs_earthquake_upload.py     # Upload USGS_Earthquake CSVs -> Supabase via JDBC
│   │   ├── usgs_nwis_upload.py           # Upload USGS_NWIS CSVs -> Supabase via JDBC
│   │   └── usgs_volcano_upload.py        # Upload USGS_Volcano CSVs -> Supabase via JDBC
│   └── raw2plain/                    # Raw-to-final transforms (thin wrappers around utils/raw2plain_utils.py)
│       ├── __init__.py
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
│   ├── upload_script_factory.py      # UploadScriptFactory: table_name -> upload function
│   ├── raw2plain_script_factory.py   # Raw2PlainScriptFactory: table_name -> raw2plain function
│   └── single_etl_factory.py         # SingleEtlFactory: script name -> run function
│
├── jobs/                             # Job modules (the "what to do" layer)
│   ├── __init__.py                   # Registers jobs in JobFactory, scripts in the script factories
│   ├── create_table.py              # CREATE TABLE job (DDL from Pydantic models)
│   ├── data_quality.py              # DATA_QUALITY job (per-file_date metrics -> geo.data_quality)
│   ├── ingestion.py                 # INGESTION job (upload + raw2plain via factories)
│   └── single_etl.py                 # SINGLE_ETL job (runs one script from single_etl/)
│
├── single_etl/                       # Standalone PySpark ETL scripts (SINGLE_ETL job)
│   ├── __init__.py
│   └── template_etl.py               # Template: source -> convert -> target with historical/incremental load
│
├── model/                            # Data models and configuration
│   ├── enums.py                      # JobType (CREATE, INGESTION, DATA_QUALITY, SINGLE_ETL) + LoadType (HISTORICAL, INCREMENTAL)
│   ├── config/
│   │   ├── app_config.py             # AppConfig, SparkConfig, DatabaseConfig, LoggingConfig, DataConfig
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
│   ├── plain/
│       ├── avalanche_org.py          # AvalancheOrg — typed model (int, bool, datetime, date)
│       ├── eonet_event.py            # EonetEvent — typed model (float, int, datetime, date)
│       ├── gdacs_event.py            # GdacsEvent — typed model (float, int, datetime, bool)
│       ├── ibtracs_cyclone.py        # IbtacsCyclone — typed model (float, int, datetime)
│       ├── nifc_fire.py              # NifcFire — typed model (float, str, datetime)
│       ├── openmeteo_flood.py        # OpenMeteoFlood — typed model (float, date)
│       ├── usgs_earthquake.py        # UsgsEarthquake — typed model (float, int, datetime)
│       ├── usgs_nwis.py              # UsgsNwis — typed model (date, float, double)
│       └── usgs_volcano.py           # UsgsVolcano — typed model (int, timestamp)
│   └── other/
│       └── data_quality.py           # DataQuality — metric row (str, float, datetime)
│
├── utils/                            # Utility modules
│   ├── ddl_generator.py              # Pydantic -> PostgreSQL DDL + JDBC execution
│   ├── dq_metrics.py                 # Per-file_date data quality rules + metric computation
│   ├── jdbc_retry.py                 # Bounded retry for transient pooler connection failures
│   ├── logger.py                     # setup_logger() for file-based logging
│   ├── raw2plain_utils.py            # Shared raw2plain logic (DRY: get_max_file_date, raw2plain template)
│   ├── schema_converter.py           # Pydantic -> PySpark StructType schemas
│   ├── spark_monitor.py              # Stage memory logging via Java SparkListener
│   ├── spark_listener/               # Compiled Java SparkListener classes
│   │   ├── StageMemoryListener.java  # Logs JVM + Spark memory on stage completion
│   │   ├── StageMemoryListener.class
│   │   ├── ListenerHelper.java       # Py4J bridge for listener registration
│   │   └── ListenerHelper.class
│   └── upload_utils.py               # Shared upload logic (DRY: parse_file_date, get_max_file_date, upload template)
│
├── docs/                             # Documentation (this file included)
│
├── logs/                             # Runtime log output (gitignored)
│   ├── ingestion/                    # INGESTION job logs
│   ├── create_table/                 # CREATE TABLE job logs
│   ├── data_quality/                 # DATA_QUALITY job logs
│   ├── drop_table/                   # Drop table job logs
│   ├── drop_schema/                  # Drop schema job logs
│   ├── delete_last_filedate/         # Delete last file_date job logs
│   ├── memory/                       # Per-stage memory consumption logs
│   ├── upload/                       # Standalone upload job logs
│   ├── raw2plain/                    # Standalone raw2plain transform logs
│   ├── single_etl/                   # SINGLE_ETL job logs
│   ├── pyspark_csv_test/             # CSV test script logs
│   └── pyspark_supabase_test/        # Supabase test script logs
│
├── spark_cluster_image/              # Docker image for Spark cluster
│   ├── Dockerfile                    # Custom Spark image (apache/spark:3.5.5 + JARs)
│   └── jars/
│       ├── hadoop-aws-3.3.4.jar
│       ├── aws-java-sdk-bundle-1.12.262.jar
│       └── postgresql-42.7.4.jar
│
└── test/                             # Unit tests (pytest)
    ├── conftest.py                   # Shared test fixtures (mock_F, mock_app_config, mock_db_config, mock_spark, spark_session)
    ├── factory/
    │   ├── test_job_factory.py
    │   ├── test_table_factory.py
    │   ├── test_upload_script_factory.py
    │   ├── test_raw2plain_script_factory.py
    │   └── test_single_etl_factory.py
    ├── jobs/
    │   ├── test_create_table.py
    │   ├── test_data_quality.py
    │   ├── test_ingestion.py
    │   └── test_single_etl.py
    ├── model/
    │   ├── test_enums.py
    │   ├── config/
    │   │   ├── test_config.py
    │   │   └── test_supabase_config.py
    │   ├── plain/
    │   │   ├── test_avalanche_org.py
    │   │   ├── test_eonet_event.py
    │   │   ├── test_gdacs_event.py
    │   │   ├── test_ibtracs_cyclone.py
    │   │   ├── test_nifc_fire.py
    │   │   ├── test_openmeteo_flood.py
    │   │   ├── test_usgs_earthquake.py
    │   │   ├── test_usgs_nwis.py
    │   │   └── test_usgs_volcano.py
    │   ├── raw/
    │   │   ├── test_avalanche_org_raw.py
    │   │   ├── test_eonet_event_raw.py
    │   │   ├── test_gdacs_event_raw.py
    │   │   ├── test_ibtracs_cyclone_raw.py
    │   │   ├── test_nifc_fire_raw.py
    │   │   ├── test_openmeteo_flood_raw.py
    │   │   ├── test_usgs_earthquake_raw.py
    │   │   ├── test_usgs_nwis_raw.py
    │   │   └── test_usgs_volcano_raw.py
    │   └── other/
    │       └── test_data_quality.py
    ├── scripts/
    │   ├── test_run_etl.py
    │   ├── test_run_drop_table.py
    │   ├── test_run_drop_schema.py
    │   └── test_run_delete_last_filedate_data.py
    ├── ingestion/
    │   ├── upload/
    │   │   ├── test_avalanche_org_upload.py
    │   │   ├── test_eonet_event_upload.py
    │   │   ├── test_gdacs_event_upload.py
    │   │   ├── test_ibtracs_cyclone_upload.py
    │   │   ├── test_nifc_fire_upload.py
    │   │   ├── test_openmeteo_flood_upload.py
    │   │   ├── test_usgs_earthquake_upload.py
    │   │   ├── test_usgs_nwis_upload.py
    │   │   └── test_usgs_volcano_upload.py
    │   └── raw2plain/
    │       ├── test_avalanche_org_raw2plain.py
    │       ├── test_eonet_event_raw2plain.py
    │       ├── test_gdacs_event_raw2plain.py
    │       ├── test_ibtracs_cyclone_raw2plain.py
    │       ├── test_nifc_fire_raw2plain.py
    │       ├── test_openmeteo_flood_raw2plain.py
    │       ├── test_usgs_earthquake_raw2plain.py
    │       ├── test_usgs_nwis_raw2plain.py
    │       └── test_usgs_volcano_raw2plain.py
    ├── single_etl/
    │   └── test_template_etl.py
    └── util/
        ├── test_ddl_generator.py
        ├── test_dq_metrics.py
        ├── test_logger.py
        └── test_schema_converter.py
```
