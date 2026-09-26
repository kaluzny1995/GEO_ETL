"""Transform EONET event raw data to typed columns."""

import logging

from pyspark.sql import SparkSession

import utils.raw2plain_utils as _utils
from model.config import AppConfig, SupabaseConnectionConfig

RAW_TABLE = "eonet_event_raw"
PLAIN_TABLE = "eonet_event"


def _get_max_file_date(spark, jdbc_url, properties, schema):
    return _utils.get_max_file_date(spark, jdbc_url, properties, schema, PLAIN_TABLE)


def _convert(df):
    F = _utils.F
    return df.select(
        F.col("event_id"),
        F.col("event_name"),
        F.col("category_id"),
        F.col("category_name"),
        F.col("description"),
        F.col("source_id"),
        F.col("source_url"),
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        F.to_timestamp("event_date").alias("event_date"),
        F.col("magnitude_value").cast("double"),
        F.col("magnitude_unit"),
        F.to_date("closed").alias("closed"),
        F.col("geometry_count").cast("int"),
        F.col("last_latitude").cast("double"),
        F.col("last_longitude").cast("double"),
        F.to_timestamp("last_date").alias("last_date"),
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
