"""DATA_QUALITY job: computes per-file_date quality metrics for a table.

``--overall`` switches to a single snapshot of the whole table stored under
``file_date = 'overall'``; see :func:`run`.
"""

import datetime as dt
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from factory import TableFactory
from model.config import AppConfig, SupabaseConnectionConfig
from model.other.data_quality import DataQuality
from utils.ddl_generator import create_schema, execute_ddl, execute_statement, pydantic_to_ddl
from utils.dq_metrics import FILE_DATE_COL, METRICS_TABLE, OVERALL_FILE_DATE, data_quality_rows
from utils.jdbc_retry import retry_connection


def _max_stored_file_date(
    spark: SparkSession, jdbc_url: str, properties: dict[str, str], schema: str, table_name: str
) -> dt.date | None:
    """Return the most recent ``file_date`` measured for *table_name*.

    ``'overall'`` snapshot rows (see :data:`utils.dq_metrics.OVERALL_FILE_DATE`)
    are ignored — they are not dates and are stored by the ``--overall`` mode.

    Args:
        spark: Active Spark session.
        jdbc_url: JDBC connection URL.
        properties: JDBC connection properties.
        schema: PostgreSQL schema name (e.g. ``"geo"``).
        table_name: Source table whose stored metrics are looked up.

    Returns:
        The greatest stored :class:`datetime.date`, or ``None`` when no metrics
        exist for *table_name* yet (or they cannot be read) — the job then
        measures every ``file_date``. Transient connection failures are
        retried before that fallback is used.
    """
    subquery = (
        f"(SELECT MAX(TO_DATE(file_date, 'YYYY-MM-DD')) AS max_file_date "
        f"FROM {schema}.{METRICS_TABLE} WHERE table_name = '{table_name}' "
        f"AND file_date <> '{OVERALL_FILE_DATE}') AS t"
    )
    try:
        row = retry_connection(
            lambda: spark.read.jdbc(url=jdbc_url, table=subquery, properties=properties).first(),
        )
        if row and row.max_file_date:
            return row.max_file_date
    except Exception:
        pass
    return None


def run(spark: SparkSession, table_name: str, logger: logging.Logger | None = None, *, overall: bool = False) -> str:
    """Compute and store data quality metrics for a single table.

    Steps:
        1. Resolve the Pydantic model for the table and load configs.
        2. Create ``schema.data_quality`` if it does not exist.
        3. Detect the load type — **HISTORICAL** when nothing is measured yet
           (every ``file_date``), otherwise **INCREMENTAL** (only ``file_date``
           newer than the latest stored one) — and purge stored metrics whose
           ``file_date`` no longer exists in the source.
        4. Read the matching rows and compute one metric set per ``file_date``.
        5. Replace the stored rows for exactly that range.

    With ``overall=True`` the load type detection, the ``file_date`` filter and
    the purge are skipped: the **whole table** is measured as one group and
    stored under ``file_date = 'overall'``, replacing a previously stored
    snapshot (recalculated on every run). The two modes own disjoint rows —
    a normal run never deletes ``'overall'`` rows, and an ``overall`` run
    never touches the per-``file_date`` rows.

    Metrics mirror the source: re-running the job replaces the previous rows
    for *table_name*, so trends are never skewed by duplicate runs and stored
    metrics never outlive the data they describe.

    Transient connection failures to the pooler (socket timeout, unresolved
    host) are retried with a pause — see :func:`utils.jdbc_retry.retry_connection`.
    The ``DELETE`` and the append are retried as one unit, so an interrupted
    write can never leave duplicated or partial metrics behind.

    Args:
        spark: Active Spark session.
        table_name: Table to measure (e.g. ``"avalanche_org"``).
        logger: Optional logger; defaults to a module-level logger.
        overall: Measure every row of the table as a single ``'overall'``
            snapshot instead of one metric set per ``file_date``.

    Returns:
        A human-readable completion message.

    Raises:
        ValueError: If *table_name* is not registered.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    model = TableFactory.get(table_name)

    app_config = AppConfig.from_json()
    db_config = SupabaseConnectionConfig.from_json()
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(app_config.database.jdbc_driver)
    schema = app_config.database.db_schema
    source = f"{schema}.{table_name}"

    logger.info(f"Creating table '{schema}.{METRICS_TABLE}' if not exists...")
    retry_connection(lambda: create_schema(spark, schema, jdbc_url, properties), logger=logger)
    retry_connection(
        lambda: execute_ddl(spark, pydantic_to_ddl(DataQuality, METRICS_TABLE, schema=schema), jdbc_url, properties),
        logger=logger,
    )

    max_stored: dt.date | None = None
    if overall:
        logger.info(f"OVERALL: measuring every row of {source} as a single '{OVERALL_FILE_DATE}' snapshot...")
    else:
        max_stored = _max_stored_file_date(spark, jdbc_url, properties, schema, table_name)
        if max_stored is None:
            logger.info(f"No metrics stored for {table_name} → HISTORICAL: measuring every file_date")
        else:
            logger.info(f"Latest measured file_date: {max_stored} → INCREMENTAL: measuring file_date > {max_stored}")

        if max_stored is not None:
            logger.info(f"Purging metrics for file_dates no longer present in {source}...")
            purge_sql = (
                f"DELETE FROM {schema}.{METRICS_TABLE} AS q WHERE q.table_name = '{table_name}' "
                f"AND q.file_date <> '{OVERALL_FILE_DATE}' "
                f"AND NOT EXISTS (SELECT 1 FROM {source} s WHERE s.file_date = q.file_date)"
            )
            retry_connection(lambda: execute_statement(spark, purge_sql, jdbc_url, properties), logger=logger)

    logger.info(f"Reading {source}...")
    df = retry_connection(lambda: spark.read.jdbc(url=jdbc_url, table=source, properties=properties), logger=logger)
    if max_stored is not None:
        df = df.filter(F.col(FILE_DATE_COL) > F.lit(max_stored.isoformat()))

    if not retry_connection(lambda: df.head(1), logger=logger):
        if max_stored is None:
            logger.warning(f"No data in {source} — no metrics written")
            return f"No data in {source} — no metrics written."
        logger.warning(f"No new file_date to measure for {table_name} — metrics are up to date")
        return f"No new file_date for {table_name} — metrics are up to date."

    if overall:
        load_type = "OVERALL"
    elif max_stored is None:
        load_type = "HISTORICAL"
    else:
        load_type = "INCREMENTAL"

    metrics_df = data_quality_rows(df, table_name=table_name, model=model, overall=overall).cache()
    try:
        row_count = retry_connection(metrics_df.count, logger=logger)
        if overall:
            logger.info(f"Computed {row_count} metrics for all rows of {table_name} (OVERALL)")
        else:
            file_date_count = retry_connection(
                lambda: metrics_df.select(FILE_DATE_COL).distinct().count(), logger=logger
            )
            logger.info(
                f"Computed {row_count} metrics for {file_date_count} file_date(s) in {table_name} ({load_type})"
            )

        if overall:
            delete_sql = (
                f"DELETE FROM {schema}.{METRICS_TABLE} "
                f"WHERE table_name = '{table_name}' AND file_date = '{OVERALL_FILE_DATE}'"
            )
        elif max_stored is None:
            delete_sql = (
                f"DELETE FROM {schema}.{METRICS_TABLE} "
                f"WHERE table_name = '{table_name}' AND file_date <> '{OVERALL_FILE_DATE}'"
            )
        else:
            delete_sql = (
                f"DELETE FROM {schema}.{METRICS_TABLE} "
                f"WHERE table_name = '{table_name}' AND file_date > '{max_stored.isoformat()}' "
                f"AND file_date <> '{OVERALL_FILE_DATE}'"
            )

        def _replace_stored_metrics() -> None:
            logger.info(f"Replacing stored metrics for {table_name}...")
            execute_statement(spark, delete_sql, jdbc_url, properties)
            logger.info(f"Writing to {schema}.{METRICS_TABLE}...")
            metrics_df.write.jdbc(url=jdbc_url, table=f"{schema}.{METRICS_TABLE}", mode="append", properties=properties)

        retry_connection(_replace_stored_metrics, logger=logger)
        logger.info(f"Data quality metrics complete: {row_count} rows written to {schema}.{METRICS_TABLE}")
    finally:
        metrics_df.unpersist()

    return f"Data quality ({load_type}) for {table_name} written to {schema}.{METRICS_TABLE} — {row_count} metrics."
