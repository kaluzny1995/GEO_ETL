"""Upload NIFC fire CSV files to Supabase."""

import logging

from pyspark.sql import SparkSession

from model.config import AppConfig, SupabaseConnectionConfig
from model.raw import NifcFireRaw
from utils.upload_utils import get_max_file_date, parse_file_date
from utils.upload_utils import upload as _upload

TABLE_NAME = "nifc_fire_raw"


def _parse_file_date(filepath):
    return parse_file_date(filepath)


def _get_max_file_date(spark, jdbc_url, properties, schema):
    return get_max_file_date(spark, jdbc_url, properties, schema, TABLE_NAME)


def upload(
    spark: SparkSession, config: AppConfig, db_config: SupabaseConnectionConfig, logger: logging.Logger | None = None
):
    """Upload CSV files for this data source to Supabase.

    Delegates to :func:`utils.upload_utils.upload` with the dataset-specific
    table name, Pydantic model, and folder name.

    Args:
        spark: Active Spark session.
        config: Application configuration.
        db_config: Supabase connection credentials.
        logger: Optional logger for consolidated logging.
    """
    return _upload(spark, config, db_config, TABLE_NAME, NifcFireRaw, "NIFC", logger)
