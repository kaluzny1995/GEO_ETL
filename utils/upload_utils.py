"""Shared CSV-to-Supabase ingestion logic.

Provides helpers for parsing file dates, querying the latest loaded date,
and the template ``upload()`` runner used by all upload scripts. The script
interface lives in :class:`~ingestion.upload.upload_script.UploadScript`.
"""

import datetime as dt
import glob
import logging
import os
from collections.abc import Callable

import findspark

findspark.init()

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from model.config import AppConfig, SupabaseConnectionConfig
from utils.logger import setup_logger


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
    source_table: str | None = None,
) -> dt.date | None:
    """Return the most recent ``file_date`` value in *table_name*.

    Args:
        spark: Active Spark session.
        jdbc_url: JDBC connection URL for the target database.
        properties: JDBC connection properties (e.g. ``user``, ``password``).
        schema: PostgreSQL schema name (e.g. ``"geo"``).
        table_name: Fully qualified table name without schema prefix.
        source_table: When set, only rows whose ``source_name`` also occurs in
            ``schema.source_table`` are considered — every ``*_etl`` table
            stamps its own ``source_name``, so scoping the target to the
            labels of one source table gives that source's incremental
            threshold. ``None`` (the default) uses every row of *table_name*.

    Returns:
        The maximum :class:`datetime.date`, or ``None`` when the table is empty
        or does not exist.
    """
    where = (
        f" WHERE source_name IN (SELECT DISTINCT source_name FROM {schema}.{source_table})"
        if source_table is not None
        else ""
    )
    subquery = f"(SELECT MAX(TO_DATE(file_date, 'YYYY-MM-DD')) AS max_file_date FROM {schema}.{table_name}{where}) AS t"
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
    dataset_folder: str,
    convert_fn: Callable[[DataFrame], DataFrame],
    logger: logging.Logger | None = None,
) -> bool:
    """Convert CSV files and write the typed rows directly to a plain table.

    Automatically detects whether to run a **HISTORICAL** load (all files)
    or an **INCREMENTAL** load (only files newer than the latest
    ``file_date`` already in the table).

    Each CSV is read with its header and all columns as text, stamped with the
    ``file_date`` parsed from its filename, and passed through *convert_fn*,
    which casts the source columns to the plain table's types. The converted
    frames are unioned and appended to *table_name* in a single JDBC write.

    Args:
        spark: Active Spark session.
        config: Application configuration (Spark, DB, logging, data paths).
        db_config: Supabase connection credentials.
        table_name: Target plain table name (e.g. ``"avalanche_org"``).
        dataset_folder: Subdirectory under the configured data path containing
            the CSV files (e.g. ``"Avalanche_ORG"``).
        convert_fn: Dataset-specific function that accepts the source CSV
            :class:`~pyspark.sql.DataFrame` and returns a typed DataFrame in
            the plain table's column order.
        logger: Optional logger; when ``None`` a file-based logger is created.

    Returns:
        ``True`` when at least one CSV file was written to the table, ``False``
        when there was nothing to ingest (no file newer than the table's latest
        ``file_date``). The caller turns ``False`` into a job failure.
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
        return False

    df = None
    for csv_path in csv_files:
        file_date = parse_file_date(csv_path)
        file_date_str = file_date.isoformat() if file_date else None
        partial_df = spark.read.csv(csv_path, header=True)
        partial_df = partial_df.withColumn("file_date", F.lit(file_date_str))
        df = partial_df if df is None else df.unionByName(partial_df)

    if df is None:  # unreachable: csv_files is non-empty, so the loop always builds df
        raise RuntimeError(f"No CSV file was read for {table_name} — nothing to write")  # pragma: no cover

    plain_df = convert_fn(df)
    full_table = f"{schema}.{table_name}"
    logger.info(f"Writing to {full_table}...")
    plain_df.write.jdbc(url=jdbc_url, table=full_table, mode="append", properties=properties)
    logger.info(f"Upload complete: {len(csv_files)} files written to {full_table}")
    return True
