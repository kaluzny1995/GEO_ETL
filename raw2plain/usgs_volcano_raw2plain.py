import logging

from pyspark.sql import SparkSession

from model.config import AppConfig, SupabaseConnectionConfig
import utils.raw2plain_utils as _utils


RAW_TABLE = "usgs_volcano_raw"
PLAIN_TABLE = "usgs_volcano"


def _get_max_file_date(spark, jdbc_url, properties, schema):
    return _utils.get_max_file_date(spark, jdbc_url, properties, schema, PLAIN_TABLE)


def _convert(df):
    F = _utils.F
    return df.select(
        F.col("volcano_name"),
        F.col("vnum").cast("int"),
        F.col("observatory"),
        F.col("observatory_name"),
        F.col("alert_level"),
        F.col("color_code"),
        F.col("notice_type"),
        F.col("notice_identifier"),
        F.to_timestamp("notice_date").alias("notice_date"),
        F.col("notice_url"),
        F.col("notice_data_url"),
        F.col("file_date"),
    )


def raw2plain(spark: SparkSession, config: AppConfig, db_config: SupabaseConnectionConfig, logger: logging.Logger | None = None):
    _utils.raw2plain(spark, config, db_config, RAW_TABLE, PLAIN_TABLE, _convert, logger)
