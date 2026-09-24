import logging

from pyspark.sql import SparkSession

from model.config import AppConfig, SupabaseConnectionConfig
from model.raw import UsgsVolcanoRaw
from utils.upload_utils import parse_file_date, get_max_file_date, upload as _upload


TABLE_NAME = "usgs_volcano_raw"


def _parse_file_date(filepath):
    return parse_file_date(filepath)


def _get_max_file_date(spark, jdbc_url, properties, schema):
    return get_max_file_date(spark, jdbc_url, properties, schema, TABLE_NAME)


def upload(spark: SparkSession, config: AppConfig, db_config: SupabaseConnectionConfig, logger: logging.Logger | None = None):
    _upload(spark, config, db_config, TABLE_NAME, UsgsVolcanoRaw, "USGS_Volcano", logger)
