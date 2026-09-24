import logging

from pyspark.sql import SparkSession

from model.config import AppConfig, SupabaseConnectionConfig
import utils.raw2plain_utils as _utils


RAW_TABLE = "usgs_earthquake_raw"
PLAIN_TABLE = "usgs_earthquake"


def _get_max_file_date(spark, jdbc_url, properties, schema):
    return _utils.get_max_file_date(spark, jdbc_url, properties, schema, PLAIN_TABLE)


def _convert(df):
    F = _utils.F
    return df.select(
        F.to_timestamp("time").alias("time"),
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        F.col("depth").cast("double"),
        F.col("mag").cast("double"),
        F.col("magType").alias("mag_type"),
        F.col("nst").cast("int"),
        F.col("gap").cast("int"),
        F.col("dmin").cast("double"),
        F.col("rms").cast("double"),
        F.col("net"),
        F.col("id").alias("event_id"),
        F.to_timestamp("updated").alias("updated"),
        F.col("place"),
        F.col("type").alias("event_type"),
        F.col("horizontalError").cast("double").alias("horizontal_error"),
        F.col("depthError").cast("double").alias("depth_error"),
        F.col("magError").cast("double").alias("mag_error"),
        F.col("magNst").cast("int").alias("mag_nst"),
        F.col("status"),
        F.col("locationSource").alias("location_source"),
        F.col("magSource").alias("mag_source"),
        F.col("file_date"),
    )


def raw2plain(spark: SparkSession, config: AppConfig, db_config: SupabaseConnectionConfig, logger: logging.Logger | None = None):
    _utils.raw2plain(spark, config, db_config, RAW_TABLE, PLAIN_TABLE, _convert, logger)
