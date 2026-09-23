import logging

from model.config import AppConfig, SupabaseConnectionConfig
from factory import UploadScriptFactory, Raw2PlainScriptFactory


def run(spark, table_name: str, logger: logging.Logger | None = None) -> str:
    if logger is None:
        logger = logging.getLogger(__name__)

    app_config = AppConfig.from_json()
    db_config = SupabaseConnectionConfig.from_json()

    # Step 1: Upload raw CSVs -> raw table
    upload_fn = UploadScriptFactory.get(table_name)
    logger.info(f"Running upload for {table_name}...")
    upload_fn(spark, app_config, db_config, logger=logger)
    logger.info(f"Upload for {table_name} complete")

    # Step 2: Transform raw -> plain table
    raw2plain_fn = Raw2PlainScriptFactory.get(table_name)
    logger.info(f"Running raw2plain for {table_name}...")
    raw2plain_fn(spark, app_config, db_config, logger=logger)
    logger.info(f"Raw2Plain for {table_name} complete")

    return f"Ingestion for {table_name} complete."
