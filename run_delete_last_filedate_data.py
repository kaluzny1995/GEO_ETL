"""Delete the latest file_date from both raw and plain tables.

Useful for re-running INCREMENTAL ingestion tests.

Usage::

    python run_delete_last_filedate_data.py <table_name>
"""

import argparse
import datetime as dt
import os

import findspark

findspark.init()

from pyspark.sql import SparkSession

from model.config import AppConfig, SupabaseConnectionConfig
from utils.ddl_generator import _get_jdbc_connection
from utils.logger import setup_logger


def _delete_file_date(
    spark: SparkSession,
    jdbc_url: str,
    properties: dict[str, str],
    schema: str,
    table_name: str,
    file_date: str,
) -> int:
    """Delete all rows matching *file_date* and return the deleted count.

    Args:
        spark: Active Spark session.
        jdbc_url: JDBC connection URL.
        properties: JDBC connection properties.
        schema: PostgreSQL schema name.
        table_name: Target table name.
        file_date: The ``file_date`` value to delete (``"YYYY-MM-DD"``).

    Returns:
        Number of rows deleted.
    """
    count_sql = f"(SELECT COUNT(*) AS cnt FROM {schema}.{table_name} WHERE file_date = '{file_date}') AS t"
    row = spark.read.jdbc(url=jdbc_url, table=count_sql, properties=properties).first()
    if row is None:
        raise RuntimeError(f"COUNT returned no row for {schema}.{table_name} on {file_date}")
    before = row.cnt

    conn = _get_jdbc_connection(spark, jdbc_url, properties)
    try:
        stmt = conn.createStatement()
        stmt.executeUpdate(f"DELETE FROM {schema}.{table_name} WHERE file_date = '{file_date}'")
    finally:
        conn.close()

    return before


def _get_max_file_date(
    spark: SparkSession,
    jdbc_url: str,
    properties: dict[str, str],
    schema: str,
    table_name: str,
) -> str | None:
    """Return the most recent ``file_date`` as an ISO string, or ``None``.

    Args:
        spark: Active Spark session.
        jdbc_url: JDBC connection URL.
        properties: JDBC connection properties.
        schema: PostgreSQL schema name.
        table_name: Target table name.

    Returns:
        The maximum ``file_date`` in ``"YYYY-MM-DD"`` format, or ``None``
        if the table is empty.
    """
    subquery = f"(SELECT MAX(TO_DATE(file_date, 'YYYY-MM-DD')) AS max_file_date FROM {schema}.{table_name}) AS t"
    try:
        row = spark.read.jdbc(url=jdbc_url, table=subquery, properties=properties).first()
        if row and row.max_file_date:
            return row.max_file_date.isoformat()
    except Exception:
        pass
    return None


def main() -> None:
    """Parse CLI arguments, connect to Spark, and delete the latest file_date."""
    app_config = AppConfig.from_json()
    db_config = SupabaseConnectionConfig.from_json()

    parser = argparse.ArgumentParser(description="Delete data for the latest file_date from raw and plain tables")
    parser.add_argument("table_name", help="Table name (e.g. avalanche_org — _raw suffix added automatically)")
    args = parser.parse_args()

    raw_table = args.table_name if args.table_name.endswith("_raw") else args.table_name + "_raw"
    # Derive plain table name by stripping _raw suffix
    plain_table = raw_table.removesuffix("_raw")

    log_dir = app_config.logging.directory
    app_prefix = app_config.spark.app_name_prefix

    logger = setup_logger(
        name=f"delete_last_filedate_{raw_table}",
        log_file=os.path.join(
            log_dir, "delete_last_filedate", f"delete_last_filedate_{raw_table}_{dt.datetime.now().isoformat()}.log"
        ),
        level=app_config.logging.level_int,
    )
    logger.info(f"---- Delete last file_date started: {raw_table} ----")

    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(app_config.database.jdbc_driver)
    schema = app_config.database.db_schema

    spark = (
        SparkSession.builder.master(app_config.spark.master)
        .appName(f"{app_prefix}_delete_last_filedate_{raw_table}")
        .getOrCreate()
    )

    try:
        logger.info(f"Connected to Spark: {spark.sparkContext.master}")
        logger.info(f"Spark version: {spark.version}")

        # Find latest file_date in raw table
        max_file_date = _get_max_file_date(spark, jdbc_url, properties, schema, raw_table)
        if max_file_date is None:
            logger.info("No data in raw table. Nothing to delete.")
            return

        logger.info(f"Latest file_date: {max_file_date}")

        # Delete from plain table first (child), then raw
        plain_deleted = _delete_file_date(spark, jdbc_url, properties, schema, plain_table, max_file_date)
        logger.info(f"Deleted {plain_deleted} rows from {schema}.{plain_table}")

        raw_deleted = _delete_file_date(spark, jdbc_url, properties, schema, raw_table, max_file_date)
        logger.info(f"Deleted {raw_deleted} rows from {schema}.{raw_table}")

        logger.info(f"Delete complete: file_date={max_file_date}, raw={raw_deleted}, plain={plain_deleted}")
    except Exception:
        logger.exception("Delete last file_date job failed")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
