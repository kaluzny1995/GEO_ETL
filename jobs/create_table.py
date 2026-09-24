"""CREATE TABLE job: generates DDL from Pydantic models and executes it."""

import logging

from pyspark.sql import SparkSession

from model.config import AppConfig, SupabaseConnectionConfig
from factory import TableFactory
from utils.ddl_generator import pydantic_to_ddl, create_schema, execute_ddl


def run(spark: SparkSession, table_name: str, logger: logging.Logger | None = None) -> str:
    """Create a table in Supabase from its Pydantic model definition.

    Steps:
        1. Create the target schema if it does not exist.
        2. Generate ``CREATE TABLE IF NOT EXISTS`` DDL from the model.
        3. Execute the DDL against the database.

    Args:
        spark: Active Spark session.
        table_name: Table name to create (e.g. ``"avalanche_org_raw"``).
        logger: Optional logger; defaults to a module-level logger.

    Returns:
        A human-readable completion message.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    # Get Pydantic model for this table (raises ValueError if not registered)
    model = TableFactory.get(table_name)

    # Load configs (Spark master, DB schema, Supabase credentials)
    app_config = AppConfig.from_json()
    db_config = SupabaseConnectionConfig.from_json()

    # Build JDBC connection params
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(app_config.database.jdbc_driver)

    # Step 1: Create schema if it doesn't exist (e.g. "geo")
    schema = app_config.database.db_schema
    logger.info(f"Creating schema '{schema}' if not exists...")
    create_schema(spark, schema, jdbc_url, properties)
    logger.info(f"Schema '{schema}' ready")

    # Step 2: Generate CREATE TABLE DDL from Pydantic model
    ddl = pydantic_to_ddl(model, table_name, schema=schema)
    logger.info(f"Generated DDL for {schema}.{table_name}")

    # Step 3: Execute DDL against Supabase
    logger.info(f"Executing DDL against Supabase...")
    execute_ddl(spark, ddl, jdbc_url, properties)
    logger.info(f"DDL executed successfully")

    return f"Table {schema}.{table_name} created."
