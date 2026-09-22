import datetime as dt
import glob
import os

import findspark
findspark.init()

from pyspark.sql import SparkSession

import model  # noqa: F401 — registers tables
import jobs  # noqa: F401 — registers jobs
from model.config import AppConfig, SupabaseConnectionConfig
from model.raw import AvalancheOrgRaw
from utils.schema_converter import pydantic_to_spark_schema
from utils.logger import setup_logger


TABLE_NAME = "avalanche_org_raw"


def _get_max_file_date(spark, jdbc_url: str, properties: dict, schema: str) -> dt.date | None:
    """Query the table for the most recent file_date value."""
    subquery = f"(SELECT MAX(TO_DATE(file_date, 'YYYY-MM-DD')) AS max_file_date FROM {schema}.{TABLE_NAME}) AS t"
    try:
        row = spark.read.jdbc(url=jdbc_url, table=subquery, properties=properties).first()
        if row and row.max_file_date:
            return row.max_file_date
    except Exception:
        pass
    return None


def _parse_file_date(filepath: str) -> dt.date | None:
    """Extract date from CSV filename like 'avalanche_org_2026-09-01.csv'."""
    try:
        date_str = os.path.basename(filepath).rstrip(".csv").rsplit("_", 1)[-1]
        return dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    except (IndexError, ValueError):
        return None


def upload(spark: SparkSession, config: AppConfig, db_config: SupabaseConnectionConfig):
    """Load Avalanche_ORG CSVs and upload to Supabase.

    Automatically determines load type:
    - HISTORICAL: table is empty, uploads all CSVs
    - INCREMENTAL: table has data, uploads only CSVs newer than max file_date
    """
    schema = config.database.db_schema
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(config.database.jdbc_driver)

    logger = setup_logger(
        name=f"upload_{TABLE_NAME}",
        log_file=os.path.join(
            config.logging.directory, "upload",
            f"upload_{TABLE_NAME}_{dt.datetime.now().isoformat()}.log",
        ),
        level=config.logging.level_int,
    )
    logger.info(f"---- Upload started: {TABLE_NAME} ----")

    data_dir = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), config.data.directory))
    avalanche_dir = os.path.join(data_dir, "Avalanche_ORG")
    logger.info(f"Data directory: {avalanche_dir}")

    all_csvs = sorted(glob.glob(os.path.join(avalanche_dir, "*.csv")))
    logger.info(f"Found {len(all_csvs)} CSV files")

    # Determine load type
    max_file_date = _get_max_file_date(spark, jdbc_url, properties, schema)

    if max_file_date is None:
        logger.info("Table empty → HISTORICAL: loading all files")
        csv_files = all_csvs
    else:
        logger.info(f"Latest file_date in table: {max_file_date} → INCREMENTAL")
        csv_files = [f for f in all_csvs if (_parse_file_date(f) or dt.date.min) > max_file_date]
        logger.info(f"{len(csv_files)} new files to upload")

    if not csv_files:
        logger.info("No files to upload. Done.")
        return

    spark_schema = pydantic_to_spark_schema(AvalancheOrgRaw)
    df = spark.read.csv(csv_files, header=True, schema=spark_schema)
    record_count = df.count()
    logger.info(f"Total records to upload: {record_count}")

    full_table = f"{schema}.{TABLE_NAME}"
    logger.info(f"Writing to {full_table}...")
    df.write.jdbc(url=jdbc_url, table=full_table, mode="append", properties=properties)
    logger.info(f"Upload complete: {record_count} records written to {full_table}")
