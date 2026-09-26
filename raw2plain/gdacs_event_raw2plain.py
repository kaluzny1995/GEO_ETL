"""Transform GDACS event raw data to typed columns."""

import logging

from pyspark.sql import SparkSession

import utils.raw2plain_utils as _utils
from model.config import AppConfig, SupabaseConnectionConfig

RAW_TABLE = "gdacs_event_raw"
PLAIN_TABLE = "gdacs_event"


def _get_max_file_date(spark, jdbc_url, properties, schema):
    return _utils.get_max_file_date(spark, jdbc_url, properties, schema, PLAIN_TABLE)


def _convert(df):
    F = _utils.F
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


def raw2plain(
    spark: SparkSession, config: AppConfig, db_config: SupabaseConnectionConfig, logger: logging.Logger | None = None
):
    """Transform raw data to typed columns for this data source.

    Delegates to :func:`utils.raw2plain_utils.raw2plain` with the
    dataset-specific convert function.

    Args:
        spark: Active Spark session.
        config: Application configuration.
        db_config: Supabase connection credentials.
        logger: Optional logger for consolidated logging.
    """
    _utils.raw2plain(spark, config, db_config, RAW_TABLE, PLAIN_TABLE, _convert, logger)
