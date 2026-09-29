"""INGESTION job: uploads CSV data then transforms raw → plain tables."""

import logging

from pyspark.sql import SparkSession

from factory import Raw2PlainScriptFactory, UploadScriptFactory
from model.config import AppConfig, SupabaseConnectionConfig


def run(spark: SparkSession, table_name: str, logger: logging.Logger | None = None) -> str:
    """Execute the full ingestion pipeline for a single table.

    Steps:
        1. Upload CSV files from disk to the raw JDBC table.
        2. Transform raw rows into typed columns and write to the plain table.

    The job **fails** when neither step wrote anything — i.e. there is no CSV
    file newer than the raw table's latest ``file_date`` and no raw row newer
    than the plain table's latest ``file_date``. A step that still had work
    (e.g. raw rows left untransformed by an earlier crash) keeps the job green.

    Args:
        spark: Active Spark session.
        table_name: Raw table name (e.g. ``"avalanche_org_raw"``).
        logger: Optional logger; defaults to a module-level logger.

    Returns:
        A human-readable completion message.

    Raises:
        RuntimeError: When there is nothing new to ingest for this table.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    app_config = AppConfig.from_json()
    db_config = SupabaseConnectionConfig.from_json()

    # Step 1: Upload raw CSVs -> raw table
    upload_fn = UploadScriptFactory.get(table_name)
    logger.info(f"Running upload for {table_name}...")
    uploaded = upload_fn(spark, app_config, db_config, logger=logger)
    logger.info(f"Upload for {table_name} complete")

    # Step 2: Transform raw -> plain table
    raw2plain_fn = Raw2PlainScriptFactory.get(table_name)
    logger.info(f"Running raw2plain for {table_name}...")
    transformed = raw2plain_fn(spark, app_config, db_config, logger=logger)
    logger.info(f"Raw2Plain for {table_name} complete")

    if not uploaded and not transformed:
        message = f"No new data to ingest for {table_name} — no file newer than the latest file_date"
        logger.warning(message)
        raise RuntimeError(message)

    return f"Ingestion for {table_name} complete."
