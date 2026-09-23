import datetime as dt
import logging
import os

import findspark
findspark.init()

from pyspark.sql import SparkSession, functions as F

from model.config import AppConfig, SupabaseConnectionConfig
from utils.logger import setup_logger

RAW_TABLE = "gdacs_event_raw"
PLAIN_TABLE = "gdacs_event"


def _get_max_file_date(spark, jdbc_url: str, properties: dict, schema: str) -> dt.date | None:
    """Query the plain table for the most recent file_date value."""
    subquery = f"(SELECT MAX(TO_DATE(file_date, 'YYYY-MM-DD')) AS max_file_date FROM {schema}.{PLAIN_TABLE}) AS t"
    try:
        row = spark.read.jdbc(url=jdbc_url, table=subquery, properties=properties).first()
        if row and row.max_file_date:
            return row.max_file_date
    except Exception:
        pass
    return None


def _convert(df):
    """Cast raw TEXT columns to final types."""
    return df.select(
        F.col("event_type"),
        F.col("event_id").cast("int"),
        F.col("event_name"),
        F.col("alert_level"),
        F.col("country"),
        F.col("country_code"),
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        F.to_timestamp("from_date").alias("from_date"),
        F.to_timestamp("to_date").alias("to_date"),
        F.to_timestamp("date_modified").alias("date_modified"),
        F.col("severity").cast("double"),
        F.col("severity_text"),
        F.col("severity_unit"),
        F.col("source"),
        F.col("report_url"),
        F.when(F.col("is_current").isin("true", "True", "1", "yes"), F.lit(True))
         .otherwise(F.lit(False))
         .alias("is_current"),
        F.col("file_date"),
    )


def raw2plain(spark: SparkSession, config: AppConfig, db_config: SupabaseConnectionConfig, logger: logging.Logger | None = None):
    """Transform data from gdacs_event_raw to gdacs_event.

    Automatically determines load type:
    - HISTORICAL: plain table empty, processes all raw data
    - INCREMENTAL: plain table has data, processes only raw rows with file_date > max in plain
    """
    schema = config.database.db_schema
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(config.database.jdbc_driver)

    if logger is None:
        logger = setup_logger(
            name=f"raw2plain_{PLAIN_TABLE}",
            log_file=os.path.join(
                config.logging.directory, "raw2plain",
                f"raw2plain_{PLAIN_TABLE}_{dt.datetime.now().isoformat()}.log",
            ),
            level=config.logging.level_int,
        )
    logger.info(f"---- Raw2Final started: {RAW_TABLE} → {PLAIN_TABLE} ----")

    max_file_date = _get_max_file_date(spark, jdbc_url, properties, schema)

    if max_file_date is None:
        logger.info("Plain table empty → HISTORICAL: loading all raw data")
        raw_df = spark.read.jdbc(url=jdbc_url, table=f"{schema}.{RAW_TABLE}", properties=properties)
    else:
        logger.info(f"Latest file_date in plain: {max_file_date} → INCREMENTAL")
        raw_df = spark.read.jdbc(
            url=jdbc_url,
            table=f"{schema}.{RAW_TABLE}",
            properties=properties,
        ).filter(F.col("file_date") > F.lit(max_file_date.isoformat()))

    record_count = raw_df.count()
    logger.info(f"Records to process: {record_count}")

    if record_count == 0:
        logger.warning(f"No new records to process for {PLAIN_TABLE}")
        raise ValueError(f"No new records to process for {PLAIN_TABLE}")

    plain_df = _convert(raw_df)
    logger.info(f"Writing to {schema}.{PLAIN_TABLE}...")
    plain_df.write.jdbc(url=jdbc_url, table=f"{schema}.{PLAIN_TABLE}", mode="append", properties=properties)
    logger.info(f"Raw2Plain complete: {record_count} records written to {schema}.{PLAIN_TABLE}")
