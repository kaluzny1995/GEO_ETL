import datetime as dt
import glob
import logging
import os

import findspark
findspark.init()

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType

from model.config import AppConfig, SupabaseConnectionConfig
from model.raw import UsgsEarthquakeRaw
from utils.schema_converter import pydantic_to_spark_schema
from utils.logger import setup_logger


TABLE_NAME = "usgs_earthquake_raw"


def _get_max_file_date(spark, jdbc_url: str, properties: dict, schema: str) -> dt.date | None:
    subquery = f"(SELECT MAX(TO_DATE(file_date, 'YYYY-MM-DD')) AS max_file_date FROM {schema}.{TABLE_NAME}) AS t"
    try:
        row = spark.read.jdbc(url=jdbc_url, table=subquery, properties=properties).first()
        if row and row.max_file_date:
            return row.max_file_date
    except Exception:
        pass
    return None


def _parse_file_date(filepath: str) -> dt.date | None:
    try:
        date_str = os.path.basename(filepath).rstrip(".csv").rsplit("_", 1)[-1]
        return dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    except (IndexError, ValueError):
        return None


def upload(spark: SparkSession, config: AppConfig, db_config: SupabaseConnectionConfig, logger: logging.Logger | None = None):
    schema = config.database.db_schema
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(config.database.jdbc_driver)

    if logger is None:
        logger = setup_logger(
            name=f"upload_{TABLE_NAME}",
            log_file=os.path.join(
                config.logging.directory, "upload",
                f"upload_{TABLE_NAME}_{dt.datetime.now().isoformat()}.log",
            ),
            level=config.logging.level_int,
        )
    logger.info(f"---- Upload started: {TABLE_NAME} ----")

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.normpath(os.path.join(project_root, config.data.directory))
    usgs_dir = os.path.join(data_dir, "USGS_Earthquake")
    logger.info(f"Data directory: {usgs_dir}")

    all_csvs = sorted(glob.glob(os.path.join(usgs_dir, "*.csv")))
    logger.info(f"Found {len(all_csvs)} CSV files")

    max_file_date = _get_max_file_date(spark, jdbc_url, properties, schema)

    if max_file_date is None:
        logger.info("Table empty → HISTORICAL: loading all files")
        csv_files = all_csvs
    else:
        logger.info(f"Latest file_date in table: {max_file_date} → INCREMENTAL")
        csv_files = [f for f in all_csvs if (_parse_file_date(f) or dt.date.min) > max_file_date]
        logger.info(f"{len(csv_files)} new files to upload")

    if not csv_files:
        logger.warning(f"No new files to upload for {TABLE_NAME}")
        raise ValueError(f"No new files to upload for {TABLE_NAME}")

    frames = []
    for csv_path in csv_files:
        file_date = _parse_file_date(csv_path)
        file_date_str = file_date.isoformat() if file_date else None
        csv_schema = pydantic_to_spark_schema(UsgsEarthquakeRaw)
        fields_without_file_date = [f for f in csv_schema.fields if f.name != "file_date"]
        partial_schema = StructType(fields_without_file_date)
        partial_df = spark.read.csv(csv_path, header=True, schema=partial_schema)
        partial_df = partial_df.withColumn("file_date", F.lit(file_date_str))
        frames.append(partial_df)

    df = frames[0] if len(frames) == 1 else frames[0].unionByName(frames[1])
    for frame in frames[2:]:
        df = df.unionByName(frame)

    record_count = df.count()
    logger.info(f"Total records to upload: {record_count}")

    full_table = f"{schema}.{TABLE_NAME}"
    logger.info(f"Writing to {full_table}...")
    df.write.jdbc(url=jdbc_url, table=full_table, mode="append", properties=properties)
    logger.info(f"Upload complete: {record_count} records written to {full_table}")
