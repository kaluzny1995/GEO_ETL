"""Upload IBTrACS cyclone CSV files to Supabase."""
import logging

from pyspark.sql import SparkSession

from model.config import AppConfig, SupabaseConnectionConfig
from model.raw import IbtacsCycloneRaw
from utils.upload_utils import parse_file_date, get_max_file_date, upload as _upload


TABLE_NAME = "ibtracs_cyclone_raw"


def _parse_file_date(filepath):
    return parse_file_date(filepath)


def _get_max_file_date(spark, jdbc_url, properties, schema):
    return get_max_file_date(spark, jdbc_url, properties, schema, TABLE_NAME)


def upload(spark: SparkSession, config: AppConfig, db_config: SupabaseConnectionConfig, logger: logging.Logger | None = None):
    """Upload CSV files for this data source to Supabase.

    Delegates to :func:`utils.upload_utils.upload` with the dataset-specific
    table name, Pydantic model, and folder name.

    Args:
        spark: Active Spark session.
        config: Application configuration.
        db_config: Supabase connection credentials.
        logger: Optional logger for consolidated logging.
    """
    _upload(spark, config, db_config, TABLE_NAME, IbtacsCycloneRaw, "IBTrACS", logger)
