"""Shared raw-to-plain transformation logic for all data sources.

Provides the template ``raw2plain()`` runner that reads from a raw JDBC
table, applies the script's converter, and writes typed results to the
corresponding plain table. The script interface lives in
:class:`~ingestion.raw2plain.raw2plain_script.Raw2PlainScript`.
"""

import datetime as dt
import logging
import os
from collections.abc import Callable

import findspark

findspark.init()

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from model.config import AppConfig, SupabaseConnectionConfig
from utils.logger import setup_logger


def get_max_file_date(
    spark: SparkSession,
    jdbc_url: str,
    properties: dict[str, str],
    schema: str,
    table_name: str,
) -> dt.date | None:
    """Return the most recent ``file_date`` value in *table_name*.

    Args:
        spark: Active Spark session.
        jdbc_url: JDBC connection URL for the target database.
        properties: JDBC connection properties (e.g. ``user``, ``password``).
        schema: PostgreSQL schema name (e.g. ``"geo"``).
        table_name: Target table name (without schema prefix).

    Returns:
        The maximum :class:`datetime.date`, or ``None`` when the table is empty
        or does not exist.
    """
    subquery = f"(SELECT MAX(TO_DATE(file_date, 'YYYY-MM-DD')) AS max_file_date FROM {schema}.{table_name}) AS t"
    try:
        row = spark.read.jdbc(url=jdbc_url, table=subquery, properties=properties).first()
        if row and row.max_file_date:
            return row.max_file_date
    except Exception:
        pass
    return None


def raw2plain(
    spark: SparkSession,
    config: AppConfig,
    db_config: SupabaseConnectionConfig,
    raw_table: str,
    plain_table: str,
    convert_fn: Callable[[DataFrame], DataFrame],
    logger: logging.Logger | None = None,
) -> bool:
    """Read new rows from a raw table, convert them, and write to a plain table.

    Automatically detects whether to run a **HISTORICAL** load (all raw data)
    or an **INCREMENTAL** load (only rows with ``file_date`` newer than the
    latest in the plain table).

    Args:
        spark: Active Spark session.
        config: Application configuration (Spark, DB, logging, data paths).
        db_config: Supabase connection credentials.
        raw_table: Source raw table name (e.g. ``"avalanche_org_raw"``).
        plain_table: Target plain table name (e.g. ``"avalanche_org"``).
        convert_fn: Dataset-specific function that accepts a raw
            :class:`~pyspark.sql.DataFrame` and returns a typed DataFrame.
        logger: Optional logger; when ``None`` a file-based logger is created.

    Returns:
        ``True`` when converted rows were written to the plain table, ``False``
        when there was nothing to transform (no raw row newer than the plain
        table's latest ``file_date``). The caller turns ``False`` from every
        step into a job failure.
    """
    schema = config.database.db_schema
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(config.database.jdbc_driver)

    if logger is None:
        logger = setup_logger(
            name=f"raw2plain_{plain_table}",
            log_file=os.path.join(
                config.logging.directory,
                "raw2plain",
                f"raw2plain_{plain_table}_{dt.datetime.now().isoformat()}.log",
            ),
            level=config.logging.level_int,
        )
    logger.info(f"---- Raw2Final started: {raw_table} → {plain_table} ----")

    max_file_date = get_max_file_date(spark, jdbc_url, properties, schema, plain_table)

    if max_file_date is None:
        logger.info("Plain table empty → HISTORICAL: loading all raw data")
        raw_df = spark.read.jdbc(url=jdbc_url, table=f"{schema}.{raw_table}", properties=properties)
    else:
        logger.info(f"Latest file_date in plain: {max_file_date} → INCREMENTAL")
        raw_df = spark.read.jdbc(
            url=jdbc_url,
            table=f"{schema}.{raw_table}",
            properties=properties,
        ).filter(F.col("file_date") > F.lit(max_file_date.isoformat()))

    if not raw_df.head(1):
        logger.warning(f"No new records to process for {plain_table} — nothing to do, table is up to date")
        return False

    plain_df = convert_fn(raw_df)
    logger.info(f"Writing to {schema}.{plain_table}...")
    plain_df.write.jdbc(url=jdbc_url, table=f"{schema}.{plain_table}", mode="append", properties=properties)
    logger.info(f"Raw2Plain complete: written to {schema}.{plain_table}")
    return True
