"""Transform NIFC fire raw data to typed columns."""

import logging

from pyspark.sql import SparkSession

import utils.raw2plain_utils as _utils
from model.config import AppConfig, SupabaseConnectionConfig

RAW_TABLE = "nifc_fire_raw"
PLAIN_TABLE = "nifc_fire"


def _get_max_file_date(spark, jdbc_url, properties, schema):
    return _utils.get_max_file_date(spark, jdbc_url, properties, schema, PLAIN_TABLE)


def _convert(df):
    F = _utils.F
    return df.select(
        F.col("incident_name"),
        F.col("irwin_id"),
        F.col("fire_number"),
        F.col("acres").cast("double"),
        F.col("percent_contained").cast("double"),
        F.col("fire_type"),
        F.col("fire_type_label"),
        F.col("jurisdiction"),
        F.col("jurisdiction_label"),
        F.col("state"),
        F.col("po_method"),
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        F.to_timestamp(F.col("poly_date") / 1000).alias("poly_date"),
        F.to_timestamp(F.col("created_on") / 1000).alias("created_on"),
        F.to_timestamp(F.col("modified_on") / 1000).alias("modified_on"),
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
