"""Publish GEO_Download CSV metadata to Supabase from the PySpark DATA_SOURCE_FILE job.

The job runs on the Spark side (where ``/opt/geo/GEO_Download/data`` is mounted)
and writes:

* ``geo.data_source_sources`` — one row per source folder (keeps a source visible
  in GEO_API's dropdown even when it has no CSV files);
* ``geo.data_source_files`` — one row per real CSV file only.

Load types follow the rest of GEO_ETL:

* **HISTORICAL** — the source has no stored files yet (or the caller forces it):
  replace every file row for the source with the current folder contents.
* **INCREMENTAL** — files already exist: compare the folder with the stored rows
  and write only new/changed files, remove vanished files, and refresh the
  source row.

The job is best-effort from the DAG's point of view: it runs as its own task, so
a metadata failure is visible in Airflow without blocking ingestion.
"""

import csv
import dataclasses
import datetime as dt
import glob
import json
import logging
import os
from typing import Any

from pyspark.sql import SparkSession

from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import LoadType
from model.other.data_source_file import DataSourceFile, DataSourceSource
from utils.ddl_generator import create_schema, execute_statement
from utils.jdbc_retry import retry_connection
from utils.schema_converter import pydantic_to_spark_schema
from utils.upload_utils import parse_file_date

DATA_SOURCE_FILES_TABLE = "data_source_files"
DATA_SOURCE_SOURCES_TABLE = "data_source_sources"

# Composite primary key keeps one row per source/file and lets the publisher
# replace a source's files with a single DELETE + append.
_DATA_SOURCE_FILES_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    source_name TEXT NOT NULL,
    file_name TEXT NOT NULL,
    snapshot_date TEXT,
    created_at TEXT NOT NULL,
    records INTEGER NOT NULL,
    size_kb DOUBLE PRECISION NOT NULL,
    pipeline_start_date TEXT,
    PRIMARY KEY (source_name, file_name)
);
"""

# One row per source folder; this is where the dropdown's source list lives.
_DATA_SOURCE_SOURCES_DDL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    source_name TEXT PRIMARY KEY,
    pipeline_start_date TEXT,
    updated_at TEXT NOT NULL
);
"""


@dataclasses.dataclass(frozen=True)
class PublishResult:
    """Summary of one DATA_SOURCE_FILE run."""

    source_name: str
    load_type: LoadType
    existing_files: int
    written_files: int
    removed_files: int


def _project_dir() -> str:
    """Return the GEO_ETL project directory."""
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _data_dir(config: AppConfig) -> str:
    """Resolve the configured data directory against the GEO_ETL project root."""
    return os.path.normpath(os.path.join(_project_dir(), config.data.directory))


def _now_iso() -> str:
    """Return the current UTC time as ISO-8601 text."""
    return dt.datetime.now(tz=dt.UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def configured_pipeline_start_date(data_dir: str) -> str | None:
    """Return the earliest enabled source start date from GEO_Download's config.

    Args:
        data_dir: Resolved ``GEO_Download/data`` folder.

    Returns:
        The earliest ``start_date`` (``YYYY-MM-DD``) across enabled sources, or
        ``None`` when the sibling ``config.json`` is missing or malformed.
    """
    config_path = os.path.join(os.path.dirname(data_dir), "config.json")
    try:
        with open(config_path) as handle:
            config = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None

    sources = config.get("sources")
    if not isinstance(sources, dict):
        return None

    start_dates: list[str] = []
    for source in sources.values():
        if not isinstance(source, dict) or source.get("enabled") is False:
            continue
        value = source.get("start_date")
        if not isinstance(value, str):
            continue
        try:
            dt.date.fromisoformat(value)
        except ValueError:
            continue
        start_dates.append(value)
    return min(start_dates) if start_dates else None


def count_csv_records(path: str) -> int:
    """Count data rows in a CSV file, excluding the header and blank lines."""
    with open(path, encoding="utf-8", errors="replace", newline="") as handle:
        reader = csv.reader(handle)
        next(reader, None)  # header
        return sum(1 for row in reader if any(cell.strip() for cell in row))


def _created_at(timestamp: float) -> str:
    """Return a modification timestamp as ISO-8601 UTC text."""
    value = dt.datetime.fromtimestamp(timestamp, tz=dt.UTC)
    return value.isoformat(timespec="seconds").replace("+00:00", "Z")


def build_source_row(dataset_folder: str, pipeline_start_date: str | None) -> DataSourceSource:
    """Build the source-registry row published to ``geo.data_source_sources``."""
    return DataSourceSource(
        source_name=dataset_folder,
        pipeline_start_date=pipeline_start_date,
        updated_at=_now_iso(),
    )


def collect_source_file_rows(
    data_dir: str,
    dataset_folder: str,
    pipeline_start_date: str | None,
    *,
    today: dt.date | None = None,
) -> list[DataSourceFile]:
    """Collect metadata for every CSV dated before *today* in one source folder.

    The downloader writes one file per previous day (``is_date_to_previous_day``),
    so a file dated today or later is not part of the finished inventory yet and
    is skipped. Returns only real file rows; the source itself is published
    separately to ``geo.data_source_sources``.
    """
    cutoff = today or dt.date.today()
    rows: list[DataSourceFile] = []
    folder = os.path.join(data_dir, dataset_folder)
    for path in sorted(glob.glob(os.path.join(folder, "*.csv"))):
        try:
            stat = os.stat(path)
            records = count_csv_records(path)
        except (OSError, csv.Error):
            logging.getLogger(__name__).warning("Skipping unreadable data source file: %s", path)
            continue

        file_date = parse_file_date(path)
        if file_date is not None and file_date >= cutoff:
            logging.getLogger(__name__).info("Skipping data source file dated today or later: %s", path)
            continue

        rows.append(
            DataSourceFile(
                source_name=dataset_folder,
                file_name=os.path.basename(path),
                snapshot_date=file_date.isoformat() if file_date else None,
                created_at=_created_at(stat.st_mtime),
                records=records,
                size_kb=round(stat.st_size / 1024, 1),
                pipeline_start_date=pipeline_start_date,
            )
        )
    return rows


def _read_existing_files(
    spark: SparkSession,
    jdbc_url: str,
    properties: dict[str, str],
    schema: str,
    source_name: str,
    logger: logging.Logger,
) -> dict[str, Any]:
    """Return the stored file rows for one source, keyed by file name."""
    escaped_source = source_name.replace("'", "''")
    query = (
        f"(SELECT file_name, created_at, snapshot_date, records, size_kb "
        f"FROM {schema}.{DATA_SOURCE_FILES_TABLE} "
        f"WHERE source_name = '{escaped_source}') AS t"
    )
    frame = retry_connection(
        lambda: spark.read.jdbc(url=jdbc_url, table=query, properties=properties),
        logger=logger,
    )
    return {row.file_name: row for row in frame.collect() if row.file_name}


def _row_changed(existing: Any, current: DataSourceFile) -> bool:
    """Tell whether a stored file row differs from the current file metadata."""
    return (
        str(existing.created_at) != current.created_at
        or existing.snapshot_date != current.snapshot_date
        or int(existing.records) != current.records
        or float(existing.size_kb) != current.size_kb
    )


def _delete_file_rows(
    spark: SparkSession,
    jdbc_url: str,
    properties: dict[str, str],
    schema: str,
    source_name: str,
    file_names: set[str],
) -> None:
    """Delete the named file rows for one source."""
    escaped_source = source_name.replace("'", "''")
    escaped_names = ", ".join("'" + name.replace("'", "''") + "'" for name in sorted(file_names))
    sql = (
        f"DELETE FROM {schema}.{DATA_SOURCE_FILES_TABLE} "
        f"WHERE source_name = '{escaped_source}' AND file_name IN ({escaped_names})"
    )
    execute_statement(spark, sql, jdbc_url, properties)


def _write_rows(
    spark: SparkSession,
    jdbc_url: str,
    properties: dict[str, str],
    schema: str,
    rows: list[DataSourceFile],
) -> None:
    """Append file rows through Spark JDBC (no-op for an empty list)."""
    if not rows:
        return
    frame = spark.createDataFrame(
        [row.model_dump() for row in rows],
        schema=pydantic_to_spark_schema(DataSourceFile),
    )
    frame.write.jdbc(
        url=jdbc_url,
        table=f"{schema}.{DATA_SOURCE_FILES_TABLE}",
        mode="append",
        properties=properties,
    )


def _delete_source_row(
    spark: SparkSession,
    jdbc_url: str,
    properties: dict[str, str],
    schema: str,
    source_name: str,
) -> None:
    """Delete one source-registry row before re-appending it."""
    escaped_source = source_name.replace("'", "''")
    sql = f"DELETE FROM {schema}.{DATA_SOURCE_SOURCES_TABLE} WHERE source_name = '{escaped_source}'"
    execute_statement(spark, sql, jdbc_url, properties)


def _write_source_row(
    spark: SparkSession,
    jdbc_url: str,
    properties: dict[str, str],
    schema: str,
    source_row: DataSourceSource,
) -> None:
    """Append one source-registry row through Spark JDBC."""
    frame = spark.createDataFrame(
        [source_row.model_dump()],
        schema=pydantic_to_spark_schema(DataSourceSource),
    )
    frame.write.jdbc(
        url=jdbc_url,
        table=f"{schema}.{DATA_SOURCE_SOURCES_TABLE}",
        mode="append",
        properties=properties,
    )


def publish_source_files(
    spark: SparkSession,
    config: AppConfig,
    db_config: SupabaseConnectionConfig,
    dataset_folder: str,
    logger: logging.Logger | None = None,
    *,
    load_type: LoadType | None = None,
    today: dt.date | None = None,
) -> PublishResult:
    """Publish one source's CSV metadata with HISTORICAL/INCREMENTAL semantics.

    Args:
        spark: Active Spark session.
        config: Application configuration.
        db_config: Supabase credentials.
        dataset_folder: Source folder under the data directory (e.g. ``"EONET"``).
        logger: Optional logger; defaults to the module logger.
        load_type: Force ``HISTORICAL`` or ``INCREMENTAL``. When ``None``, an
            empty file table means HISTORICAL and stored rows mean INCREMENTAL.
        today: Current date used to ignore files dated today or later; defaults
            to the system date.

    Returns:
        A :class:`PublishResult` with the detected load type and row counts.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    schema = config.database.db_schema
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(config.database.jdbc_driver)
    data_dir = _data_dir(config)
    pipeline_start_date = configured_pipeline_start_date(data_dir)

    current_files = {
        row.file_name: row
        for row in collect_source_file_rows(data_dir, dataset_folder, pipeline_start_date, today=today)
    }
    source_row = build_source_row(dataset_folder, pipeline_start_date)

    logger.info(f"Creating tables '{schema}.{DATA_SOURCE_FILES_TABLE}' and '{schema}.{DATA_SOURCE_SOURCES_TABLE}'...")
    retry_connection(lambda: create_schema(spark, schema, jdbc_url, properties), logger=logger)
    retry_connection(
        lambda: execute_statement(
            spark,
            _DATA_SOURCE_FILES_DDL.format(schema=schema, table=DATA_SOURCE_FILES_TABLE),
            jdbc_url,
            properties,
        ),
        logger=logger,
    )
    retry_connection(
        lambda: execute_statement(
            spark,
            _DATA_SOURCE_SOURCES_DDL.format(schema=schema, table=DATA_SOURCE_SOURCES_TABLE),
            jdbc_url,
            properties,
        ),
        logger=logger,
    )

    existing_files = _read_existing_files(spark, jdbc_url, properties, schema, dataset_folder, logger)
    effective_load_type = load_type
    if effective_load_type is None:
        effective_load_type = LoadType.HISTORICAL if not existing_files else LoadType.INCREMENTAL

    if effective_load_type is LoadType.HISTORICAL:
        removed = set(existing_files) - set(current_files)
        rows_to_write = list(current_files.values())
        logger.info(
            f"No stored files for {dataset_folder} → HISTORICAL: "
            f"replacing {len(existing_files)} row(s) with {len(current_files)} file(s)"
        )
        escaped_source = dataset_folder.replace("'", "''")
        delete_sql = f"DELETE FROM {schema}.{DATA_SOURCE_FILES_TABLE} WHERE source_name = '{escaped_source}'"

        def _replace_metadata() -> None:
            execute_statement(spark, delete_sql, jdbc_url, properties)
            _write_rows(spark, jdbc_url, properties, schema, rows_to_write)
            _delete_source_row(spark, jdbc_url, properties, schema, dataset_folder)
            _write_source_row(spark, jdbc_url, properties, schema, source_row)

        retry_connection(_replace_metadata, logger=logger)
        written_files = len(current_files)
        removed_files = len(removed)
    else:
        changed = [
            row
            for name, row in current_files.items()
            if name not in existing_files or _row_changed(existing_files[name], row)
        ]
        removed = set(existing_files) - set(current_files)
        # The empty name also removes a legacy sentinel row from older
        # deployments; new runs never write one.
        delete_names = {""} | {row.file_name for row in changed} | removed
        rows_to_write = changed
        logger.info(
            f"Stored files exist for {dataset_folder} → INCREMENTAL: "
            f"{len(changed)} new/changed file(s), {len(removed)} removed file(s)"
        )

        def _replace_metadata() -> None:
            if delete_names:
                _delete_file_rows(spark, jdbc_url, properties, schema, dataset_folder, delete_names)
            _write_rows(spark, jdbc_url, properties, schema, rows_to_write)
            _delete_source_row(spark, jdbc_url, properties, schema, dataset_folder)
            _write_source_row(spark, jdbc_url, properties, schema, source_row)

        retry_connection(_replace_metadata, logger=logger)
        written_files = len(changed)
        removed_files = len(removed)

    logger.info(
        f"Data source file metadata complete: {written_files} written, "
        f"{removed_files} removed for {dataset_folder} ({effective_load_type.value})"
    )
    return PublishResult(
        source_name=dataset_folder,
        load_type=effective_load_type,
        existing_files=len(existing_files),
        written_files=written_files,
        removed_files=removed_files,
    )
