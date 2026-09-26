"""Shared upload logic for CSV-to-Supabase ingestion.

Provides helpers for parsing file dates, querying the latest loaded date,
and a template ``upload()`` function used by all per-dataset upload scripts.
"""

import datetime as dt
import glob
import logging
import os

import findspark

findspark.init()

from pydantic import BaseModel
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from model.config import AppConfig, SupabaseConnectionConfig
from utils.logger import setup_logger
from utils.schema_converter import pydantic_to_spark_schema


def parse_file_date(filepath: str) -> dt.date | None:
    """Extract a ``YYYY-MM-DD`` date from a CSV filename.

    The date is expected to appear after the last underscore in the stem,
    e.g. ``avalanche_org_2026-09-20.csv`` → ``2026-09-20``.

    Args:
        filepath: Absolute or relative path to the CSV file.

    Returns:
        The parsed :class:`datetime.date`, or ``None`` if parsing fails.
    """
    try:
        date_str = os.path.basename(filepath).rstrip(".csv").rsplit("_", 1)[-1]
        return dt.datetime.strptime(date_str, "%Y-%m-%d").date()
    except (IndexError, ValueError):
        return None


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
        table_name: Fully qualified table name without schema prefix.

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


def upload(
    spark: SparkSession,
    config: AppConfig,
    db_config: SupabaseConnectionConfig,
    table_name: str,
    raw_model_cls: type[BaseModel],
    dataset_folder: str,
    logger: logging.Logger | None = None,
) -> None:
    """Upload CSV files to a Supabase table via JDBC.

    Automatically detects whether to run a **HISTORICAL** load (all files)
    or an **INCREMENTAL** load (only files newer than the latest
    ``file_date`` already in the table).

    Args:
        spark: Active Spark session.
        config: Application configuration (Spark, DB, logging, data paths).
        db_config: Supabase connection credentials.
        table_name: Target raw table name (e.g. ``"avalanche_org_raw"``).
        raw_model_cls: Pydantic model class defining the raw table schema.
        dataset_folder: Subdirectory under the configured data path containing
            the CSV files (e.g. ``"Avalanche_ORG"``).
        logger: Optional logger; when ``None`` a file-based logger is created.

    Returns:
        ``None``. If no new CSV files exist for an incremental load, a warning
        is logged and nothing is written (the table is already up to date).
    """
    schema = config.database.db_schema
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(config.database.jdbc_driver)

    if logger is None:
        logger = setup_logger(
            name=f"upload_{table_name}",
            log_file=os.path.join(
                config.logging.directory,
                "upload",
                f"upload_{table_name}_{dt.datetime.now().isoformat()}.log",
            ),
            level=config.logging.level_int,
        )
    logger.info(f"---- Upload started: {table_name} ----")

    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    data_dir = os.path.normpath(os.path.join(project_root, config.data.directory))
    data_folder = os.path.join(data_dir, dataset_folder)
    logger.info(f"Data directory: {data_folder}")

    all_csvs = sorted(glob.glob(os.path.join(data_folder, "*.csv")))
    logger.info(f"Found {len(all_csvs)} CSV files")

    max_file_date = get_max_file_date(spark, jdbc_url, properties, schema, table_name)

    if max_file_date is None:
        logger.info("Table empty → HISTORICAL: loading all files")
        csv_files = all_csvs
    else:
        logger.info(f"Latest file_date in table: {max_file_date} → INCREMENTAL")
        csv_files = [f for f in all_csvs if (parse_file_date(f) or dt.date.min) > max_file_date]
        logger.info(f"{len(csv_files)} new files to upload")

    if not csv_files:
        logger.warning(f"No new files to upload for {table_name} — nothing to do, table is up to date")
        return

    csv_schema = pydantic_to_spark_schema(raw_model_cls)
    fields_without_file_date = [f for f in csv_schema.fields if f.name != "file_date"]
    partial_schema = StructType(fields_without_file_date)

    df = None
    for csv_path in csv_files:
        file_date = parse_file_date(csv_path)
        file_date_str = file_date.isoformat() if file_date else None
        partial_df = spark.read.csv(csv_path, header=True, schema=partial_schema)
        partial_df = partial_df.withColumn("file_date", F.lit(file_date_str))
        df = partial_df if df is None else df.unionByName(partial_df)

    full_table = f"{schema}.{table_name}"
    if df is None:
        raise RuntimeError(f"No CSV file was read for {table_name} — nothing to write")
    logger.info(f"Writing to {full_table}...")
    df.write.jdbc(url=jdbc_url, table=full_table, mode="append", properties=properties)
    logger.info(f"Upload complete: {len(csv_files)} files written to {full_table}")
