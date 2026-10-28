"""Shared single-ETL runner for all data sources.

Provides a template ``run_etl()`` function that reads a source JDBC table,
resolves the load type from the target's latest ``file_date``, applies a
dataset-specific ``convert_fn``, deduplicates the result on the script's
dedup keys (scoped per ``file_date``), and appends it to the ETL table.
Each script in ``single_etl/`` is a :class:`~single_etl.single_etl_script.SingleEtlScript`
subclass that supplies its table names, its own ``_convert`` and its
``DEDUP_KEYS``.
"""

import datetime as dt
import logging
from collections.abc import Callable, Sequence
from typing import NamedTuple

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import LoadType
from utils.raw2plain_utils import get_max_file_date

FILE_DATE_COL = "file_date"


class EtlOutcome(NamedTuple):
    """What a single-ETL run did, so the job can decide success vs failure.

    Attributes:
        loaded: ``True`` when records were appended to the target table.
        summary: Human-readable summary — the write report when *loaded* is
            ``True`` (e.g. ``"HISTORICAL: wrote to geo.usgs_volcano_etl"``),
            or the complete failure message (starting with
            ``No records loaded into``) when *loaded* is ``False``.
    """

    loaded: bool
    summary: str


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


def dedup(df: DataFrame, keys: Sequence[str]) -> DataFrame:
    """Keep one row per *keys* + ``file_date`` combination.

    The deduplication step every ``*_etl`` run applies after
    :func:`run_etl`'s *convert_fn*. Two rows of the batch are duplicates
    **iff** they carry the same ``file_date`` and compare equal on every
    column of the natural event key declared by the script
    (:attr:`SingleEtlScript.DEDUP_KEYS`) — equality is exact (no rounding
    or normalisation) and null-safe (``NULL == NULL``); payload columns do
    not participate in the match. Each group collapses to one row, so the
    file's snapshot stays while cross-file snapshots (an event re-listed
    on later days) are never touched.

    Args:
        df: The converted canonical DataFrame.
        keys: Canonical columns identifying one natural event (must not
            include ``file_date`` — the runner scopes the dedup to each
            ``file_date`` itself).

    Returns:
        The DataFrame with every (``keys``, ``file_date``) group reduced to
        one row. When group members differ in payload only (observed for
        avalanche.org's standing product, whose ``off_season`` flag can
        conflict), one of the variants is kept.
    """
    return df.dropDuplicates([*keys, FILE_DATE_COL])


def run_etl(
    spark: SparkSession,
    config: AppConfig,
    db_config: SupabaseConnectionConfig,
    source_table: str | Sequence[str],
    target_table: str,
    convert_fn: Callable[[DataFrame], DataFrame],
    *,
    dedup_keys: Sequence[str] | None = None,
    load_type: LoadType | None = None,
    logger: logging.Logger | None = None,
) -> EtlOutcome:
    """Read ``geo.<source_table>``, convert it and append to ``geo.<target_table>``.

    *source_table* may also be a sequence of table names — the frames are then
    stacked with ``unionByName`` before *convert_fn* sees them (the ``union_etl``
    script stacks the nine ``*_etl`` tables this way).

    **Load types** (resolved by :func:`resolve_load_type`)

    - ``HISTORICAL`` — every row of *source_table* is processed (first run, or a
      full reprocess forced with ``--load-type historical``; clear the target first
      if the write must not duplicate rows).
    - ``INCREMENTAL`` — only rows whose ``file_date`` is newer than the latest
      ``file_date`` stored in *target_table*. For a sequence of sources the
      comparison is made **per source table**: the threshold is the latest
      ``file_date`` among the target rows carrying that source's own
      ``source_name`` (every ``*_etl`` table stamps one distinct label), so a
      source that lags behind the others still delivers its older files once
      it catches up; a source with no rows in *target_table* yet is
      processed in full.

    ``load_type=None`` (the ``--load-type auto`` default) leaves the decision to
    this runner: an empty target (or a target table that does not exist yet)
    means ``HISTORICAL``, otherwise ``INCREMENTAL``. Both types append to the
    target.

    **Deduplication** — when *dedup_keys* is given, the converted frame is
    passed through :func:`dedup` before the write: one row per key +
    ``file_date`` combination (the keys themselves never include
    ``file_date``, so every file's snapshot is preserved). ``dedup_keys=None``
    skips the step — the ``union_etl`` script, which stacks sources that are
    already deduplicated by their own scripts.

    Args:
        spark: Active Spark session.
        config: Application configuration (DB schema, JDBC driver, logging).
        db_config: Supabase connection credentials.
        source_table: Source plain table name (e.g. ``"usgs_volcano"``), or
            several table names to stack first (the union script); the
            INCREMENTAL threshold of each stacked table is scoped to the
            ``source_name`` its rows carry.
        target_table: Target ETL table name (e.g. ``"usgs_volcano_etl"``).
        convert_fn: Dataset-specific function that accepts the source
            :class:`~pyspark.sql.DataFrame` and returns the canonical
            21-column DataFrame.
        dedup_keys: Canonical columns identifying one natural event, or
            ``None`` to skip deduplication; combined with ``file_date`` by
            :func:`dedup` so only within-file duplicates collapse.
        load_type: Load type forced by the caller (``--load-type``), or ``None``
            for automatic detection.
        logger: Optional logger; defaults to this module's logger.

    Returns:
        The :class:`EtlOutcome` of the run: ``loaded=True`` with the write
        report, or ``loaded=False`` with the complete failure message when no
        record was loaded into *target_table* (an up-to-date incremental run,
        an empty source, or a conversion that yields zero rows). This runner
        never raises for those load outcomes — the caller decides what an
        unloaded run means; ``jobs/single_etl.py`` turns ``loaded=False`` into
        ``RuntimeError`` so ``run_etl.py`` exits with code 1, the same contract
        as INGESTION. (Infrastructure failures and an unresolvable
        *dedup_keys* column still surface as exceptions.)
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    schema = config.database.db_schema
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(config.database.jdbc_driver)

    sources: tuple[str, ...] = (source_table,) if isinstance(source_table, str) else tuple(source_table)
    source_label = source_table if isinstance(source_table, str) else f"{len(sources)} source tables"
    source_desc = f"{schema}.{source_label}" if isinstance(source_table, str) else source_label

    logger.info(f"---- Single ETL started: {source_desc} → {schema}.{target_table} ----")

    max_file_date = get_max_file_date(spark, jdbc_url, properties, schema, target_table)
    resolved = resolve_load_type(max_file_date, load_type)

    if load_type is None:
        reason = "target table empty" if max_file_date is None else f"latest file_date in target: {max_file_date}"
        logger.info(f"Load type auto-detected ({reason}) → {resolved.value}")
    elif resolved is not load_type:
        logger.info(f"{load_type.value} requested but the target table is empty → {resolved.value}")
    else:
        logger.info(f"Load type requested: {resolved.value}")

    frames: list[DataFrame] = []
    for source in sources:
        frame = spark.read.jdbc(url=jdbc_url, table=f"{schema}.{source}", properties=properties)
        if resolved is LoadType.INCREMENTAL:
            threshold = max_file_date
            if not isinstance(source_table, str):
                threshold = get_max_file_date(spark, jdbc_url, properties, schema, target_table, source_table=source)
            if threshold is not None:
                logger.info(f"Processing rows of {source} with {FILE_DATE_COL} > {threshold}...")
                frame = frame.filter(F.col(FILE_DATE_COL) > F.lit(threshold.isoformat()))
            else:
                logger.info(f"Processing every row of {source} — it has no rows in {target_table} yet...")
        else:
            logger.info(f"Processing every row of {source}...")
        frames.append(frame)

    source_df = frames[0]
    for frame in frames[1:]:
        source_df = source_df.unionByName(frame)

    if not source_df.head(1):
        return EtlOutcome(False, f"No records loaded into {target_table} — no rows to process in {source_label}")

    result_df = convert_fn(source_df)
    # The empty-check and the two dedup counts would each re-run the whole JDBC
    # read + convert lineage — cache the converted frame once and release it in a
    # finally block (the dedup below may reassign result_df; the cached original
    # is the one that must be unpersisted).
    cached_df = result_df
    cached_df.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        if not result_df.head(1):
            return EtlOutcome(False, f"No records loaded into {target_table} — {source_label} converted to zero rows")

        if dedup_keys:
            before = result_df.count()
            result_df = dedup(result_df, dedup_keys)
            removed = before - result_df.count()
            logger.info(
                f"Deduplication on ({', '.join(dedup_keys)}, {FILE_DATE_COL}) removed {removed} of {before} rows"
            )

        logger.info(f"Writing to {schema}.{target_table}...")
        result_df.write.jdbc(url=jdbc_url, table=f"{schema}.{target_table}", mode="append", properties=properties)
        logger.info(f"Single ETL complete: written to {schema}.{target_table}")
        return EtlOutcome(True, f"{resolved.value}: wrote to {schema}.{target_table}")
    finally:
        cached_df.unpersist()
