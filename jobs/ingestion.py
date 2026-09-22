import logging

from model.config import AppConfig, SupabaseConnectionConfig
from factory import UploadScriptFactory, Raw2FinalScriptFactory


def run(spark, table_name: str, logger: logging.Logger | None = None) -> str:
    if logger is None:
        logger = logging.getLogger(__name__)

    app_config = AppConfig.from_json()
    db_config = SupabaseConnectionConfig.from_json()

    # Step 1: Upload raw CSVs -> raw table
    upload_fn = UploadScriptFactory.get(table_name)
    logger.info(f"Running upload for {table_name}...")
    upload_fn(spark, app_config, db_config)
    logger.info(f"Upload for {table_name} complete")

    # Step 2: Transform raw -> final table
    raw2final_fn = Raw2FinalScriptFactory.get(table_name)
    logger.info(f"Running raw2final for {table_name}...")
    raw2final_fn(spark, app_config, db_config)
    logger.info(f"Raw2Final for {table_name} complete")

    return f"Ingestion for {table_name} complete."
