"""Transform USGS NWIS stream gauge raw data to typed columns."""

import logging

from pyspark.sql import SparkSession

import utils.raw2plain_utils as _utils
from model.config import AppConfig, SupabaseConnectionConfig

RAW_TABLE = "usgs_nwis_raw"
PLAIN_TABLE = "usgs_nwis"


def _get_max_file_date(spark, jdbc_url, properties, schema):
    return _utils.get_max_file_date(spark, jdbc_url, properties, schema, PLAIN_TABLE)


def _convert(df):
    F = _utils.F
    return df.select(
        F.col("site_id"),
        F.col("site_name"),
        F.col("parameter_code"),
        F.to_date("date").alias("date"),
        F.col("value").cast("double"),
        F.col("unit"),
        F.col("qualifiers"),
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
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
    return _utils.raw2plain(spark, config, db_config, RAW_TABLE, PLAIN_TABLE, _convert, logger)
