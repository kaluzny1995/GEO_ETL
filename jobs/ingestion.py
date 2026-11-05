"""INGESTION job: converts CSV files and loads them into plain tables."""

import logging

from pyspark.sql import SparkSession

from factory import UploadScriptFactory
from model.config import AppConfig, SupabaseConnectionConfig


def run(spark: SparkSession, table_name: str, logger: logging.Logger | None = None) -> str:
    """Execute the CSV → plain-table ingestion for a single table.

    The upload script reads every CSV newer than the table's latest
    ``file_date``, casts the text columns to the plain table's types and
    appends the converted rows in one JDBC write.

    The job **fails** when no CSV file is newer than the plain table's latest
    ``file_date``.

    Args:
        spark: Active Spark session.
        table_name: Plain table name (e.g. ``"avalanche_org"``).
        logger: Optional logger; defaults to a module-level logger.

    Returns:
        A human-readable completion message.

    Raises:
        RuntimeError: When there is nothing new to ingest for this table.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    app_config = AppConfig.from_json()
    db_config = SupabaseConnectionConfig.from_env()

    upload_script = UploadScriptFactory.get(table_name)
    logger.info(f"Running upload for {table_name}...")
    uploaded = upload_script.upload(spark, app_config, db_config, logger=logger)
    logger.info(f"Upload for {table_name} complete")

    if not uploaded:
        message = f"No new data to ingest for {table_name} — no file newer than the latest file_date"
        logger.warning(message)
        raise RuntimeError(message)

    return f"Ingestion for {table_name} complete."
