"""Shared single-ETL runner for all data sources.

Provides a template ``run_etl()`` function that reads a source JDBC table,
resolves the load type from the target's latest ``file_date``, applies a
dataset-specific ``convert_fn``, and appends the result to the ETL table.
Each script in ``single_etl/`` is a thin wrapper that supplies its table
names and its own ``_convert``.
"""

import datetime as dt
import logging
from collections.abc import Callable

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import LoadType
from utils.raw2plain_utils import get_max_file_date

FILE_DATE_COL = "file_date"


def resolve_load_type(max_file_date: dt.date | None, requested: LoadType | None) -> LoadType:
    """Decide which load type a run should use.

    Args:
        max_file_date: Latest ``file_date`` stored in the target table, or
            ``None`` when the target is empty or does not exist yet.
        requested: Load type forced on the CLI (``--load-type``), or ``None``
            for automatic detection.

    Returns:
        The load type to run with:

        * empty target → ``HISTORICAL`` (there is nothing to be incremental from),
        * explicit request → the requested type (``HISTORICAL`` forces a full reprocess),
        * otherwise → ``INCREMENTAL``.
    """
    if max_file_date is None:
        return LoadType.HISTORICAL
    if requested is not None:
        return requested
    return LoadType.INCREMENTAL


def run_etl(
    spark: SparkSession,
    config: AppConfig,
    db_config: SupabaseConnectionConfig,
    source_table: str,
    target_table: str,
    convert_fn: Callable[[DataFrame], DataFrame],
    *,
    load_type: LoadType | None = None,
    logger: logging.Logger | None = None,
) -> str:
    """Read ``geo.<source_table>``, convert it and append to ``geo.<target_table>``.

    **Load types** (resolved by :func:`resolve_load_type`)

    - ``HISTORICAL`` — every row of *source_table* is processed (first run, or a
      full reprocess forced with ``--load-type historical``; clear the target first
      if the write must not duplicate rows).
    - ``INCREMENTAL`` — only rows whose ``file_date`` is newer than the latest
      ``file_date`` stored in *target_table*.

    ``load_type=None`` (the ``--load-type auto`` default) leaves the decision to
    this runner: an empty target (or a target table that does not exist yet)
    means ``HISTORICAL``, otherwise ``INCREMENTAL``. Both types append to the
    target.

    Args:
        spark: Active Spark session.
        config: Application configuration (DB schema, JDBC driver, logging).
        db_config: Supabase connection credentials.
        source_table: Source plain table name (e.g. ``"usgs_volcano"``).
        target_table: Target ETL table name (e.g. ``"usgs_volcano_etl"``).
        convert_fn: Dataset-specific function that accepts the source
            :class:`~pyspark.sql.DataFrame` and returns the canonical
            21-column DataFrame.
        load_type: Load type forced by the caller (``--load-type``), or ``None``
            for automatic detection.
        logger: Optional logger; defaults to this module's logger.

    Returns:
        A human-readable summary, e.g. ``"HISTORICAL: wrote to geo.usgs_volcano_etl"``
        or ``"INCREMENTAL: nothing to do — usgs_volcano_etl is up to date"``.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    schema = config.database.db_schema
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(config.database.jdbc_driver)

    logger.info(f"---- Single ETL started: {schema}.{source_table} → {schema}.{target_table} ----")

    max_file_date = get_max_file_date(spark, jdbc_url, properties, schema, target_table)
    resolved = resolve_load_type(max_file_date, load_type)

    if load_type is None:
        reason = "target table empty" if max_file_date is None else f"latest file_date in target: {max_file_date}"
        logger.info(f"Load type auto-detected ({reason}) → {resolved.value}")
    elif resolved is not load_type:
        logger.info(f"{load_type.value} requested but the target table is empty → {resolved.value}")
    else:
        logger.info(f"Load type requested: {resolved.value}")

    source_df = spark.read.jdbc(url=jdbc_url, table=f"{schema}.{source_table}", properties=properties)

    if resolved is LoadType.INCREMENTAL and max_file_date is not None:
        logger.info(f"Processing rows of {source_table} with {FILE_DATE_COL} > {max_file_date}...")
        source_df = source_df.filter(F.col(FILE_DATE_COL) > F.lit(max_file_date.isoformat()))
    else:
        logger.info(f"Processing every row of {source_table}...")

    if not source_df.head(1):
        logger.warning(f"No new records for {target_table} — nothing to do, table is up to date")
        return f"{resolved.value}: nothing to do — {target_table} is up to date"

    result_df = convert_fn(source_df)
    logger.info(f"Writing to {schema}.{target_table}...")
    result_df.write.jdbc(url=jdbc_url, table=f"{schema}.{target_table}", mode="append", properties=properties)
    logger.info(f"Single ETL complete: written to {schema}.{target_table}")
    return f"{resolved.value}: wrote to {schema}.{target_table}"
