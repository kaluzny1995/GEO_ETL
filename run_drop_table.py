"""Drop a table from Supabase.

Usage::

    python run_drop_table.py <table_name>
"""

import argparse
import datetime as dt
import os

import findspark

findspark.init()

from pyspark.sql import SparkSession

from model.config import AppConfig, SupabaseConnectionConfig
from utils.ddl_generator import drop_table
from utils.logger import setup_logger


def main() -> None:
    """Parse CLI arguments, connect to Spark, and drop the specified table."""
    # Load configs
    app_config = AppConfig.from_json()
    db_config = SupabaseConnectionConfig.from_env()

    # Parse CLI arguments (positional: table_name)
    parser = argparse.ArgumentParser(description="Drop a table from Supabase")
    parser.add_argument("table_name", help="Table name to drop")
    args = parser.parse_args()

    table_name = args.table_name
    log_dir = app_config.logging.directory
    app_prefix = app_config.spark.app_name_prefix

    logger = setup_logger(
        name=f"drop_table_{table_name}",
        log_file=os.path.join(log_dir, "drop_table", f"drop_table_{table_name}_{dt.datetime.now().isoformat()}.log"),
        level=app_config.logging.level_int,
    )
    logger.info(f"---- Drop table job started: {table_name} ----")

    # Build JDBC connection params
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(app_config.database.jdbc_driver)
    schema = app_config.database.db_schema

    # Connect to Spark cluster
    spark = (
        SparkSession.builder.master(app_config.spark.master)
        .appName(f"{app_prefix}_drop_table_{table_name}")
        .getOrCreate()
    )

    try:
        logger.info(f"Connected to Spark: {spark.sparkContext.master}")
        logger.info(f"Spark version: {spark.version}")

        # Drop the table
        drop_table(spark, table_name, schema, jdbc_url, properties)
        logger.info(f"Table {schema}.{table_name} dropped.")
    except Exception:
        logger.exception("Drop table job failed")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
