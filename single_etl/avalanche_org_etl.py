"""Avalanche.org → ``geo.avalanche_org_etl`` (canonical event structure).

Reads the typed plain table ``geo.avalanche_org`` and writes every forecast
zone/day into ``geo.avalanche_org_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl`` tables.
Zone coordinates come from the source's own ``latitude``/``longitude`` columns —
polygon centroids computed by GEO_Download when the daily CSVs are written, so
this script performs no lookup and no network access.

Run::

    python run_etl.py SINGLE_ETL avalanche_org_etl
    python run_etl.py SINGLE_ETL avalanche_org_etl --load-type historical
    python run_etl.py SINGLE_ETL avalanche_org_etl --load-type incremental

**Load types** — ``HISTORICAL``, ``INCREMENTAL`` and the ``--load-type auto``
detection are described in :func:`utils.etl_utils.run_etl`.

**Adding the next source** — copy this file to ``single_etl/<name>_etl.py``,
point ``SOURCE_TABLE``/``TARGET_TABLE`` at its plain and ETL tables, rewrite
:func:`_convert` for its columns, then register it in ``jobs/__init__.py``::

    from single_etl.<name>_etl import run as <name>_etl_run
    SingleEtlFactory.register("<name>_etl", <name>_etl_run)
"""

import logging

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import LoadType
from utils.etl_utils import run_etl

SOURCE_TABLE = "avalanche_org"
TARGET_TABLE = "avalanche_org_etl"
FILE_DATE_COL = "file_date"
SOURCE_NAME = "AVALANCHE_ORG"
COUNTRY_CODE = "US"
EVENT_CATEGORY = "avalanche"
METRIC_NAME = "magnitude"


def _danger_rating() -> Column:
    """The zone's danger rating, or ``NULL`` while the zone is unrated.

    The source marks unrated zones with ``danger_level = -1`` and a rating text
    of ``no rating``; neither is a rating, so both map to ``NULL``.
    """
    rating = F.trim(F.col("danger_rating"))
    unrated = (F.col("danger_level") <= 0) | (F.lower(rating).isin("", "no rating", "no_rating"))
    return F.when(~unrated, rating)


def _convert(df: DataFrame) -> DataFrame:
    """Map ``geo.avalanche_org`` columns onto the canonical ETL structure.

    Every zone carries its polygon centroid as ``latitude``/``longitude``
    (``NULL`` when the source row has no coordinates, i.e. the download found
    no usable geometry). A danger level is a rating, not a measurement, so
    ``metric_value`` and ``metric_unit`` stay ``NULL`` and unrated zones
    (``danger_level = -1``) additionally get a ``NULL`` ``danger_rating``.

    Each source file holds the analysis day plus the next day's forecast (the
    shared ``[D, D+1]`` download range), and ``date`` is not part of the
    canonical structure — while both rows share one forecast window (the
    off-season "no rating" product), they project to identical rows. The
    trailing :meth:`DataFrame.distinct` collapses such copies, so the target
    keeps one row per forecast statement per ``file_date``; rows that differ
    (e.g. the next day's own validity window) are kept as their own snapshot.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.

    Returns:
        The DataFrame in :data:`TARGET_TABLE` column order, without duplicate rows.
    """
    return df.select(
        F.lit(SOURCE_NAME).alias("source_name"),
        F.col("center_name").alias("subsource_name"),
        F.lit(None).cast("string").alias("event_name"),
        F.lit(EVENT_CATEGORY).alias("event_category"),
        F.lit(None).cast("string").alias("event_subcategory"),
        F.lit(None).cast("string").alias("event_description"),
        F.col("latitude"),
        F.col("longitude"),
        F.lit(COUNTRY_CODE).alias("country_code"),
        F.col("state").alias("state_province"),
        F.col("zone_name").alias("place_location"),
        F.lit(METRIC_NAME).alias("metric_name"),
        F.lit(None).cast("double").alias("metric_value"),
        F.lit(None).cast("string").alias("metric_unit"),
        _danger_rating().alias("danger_rating"),
        F.lit(None).cast("string").alias("other_metrics"),
        F.col("forecast_start").alias("event_start"),
        F.col("forecast_end").alias("event_end"),
        F.lit(None).cast("double").alias("last_latitude"),
        F.lit(None).cast("double").alias("last_longitude"),
        F.col(FILE_DATE_COL),
    ).distinct()


def run(
    spark: SparkSession,
    config: AppConfig,
    db_config: SupabaseConnectionConfig,
    *,
    load_type: LoadType | None = None,
    logger: logging.Logger | None = None,
) -> str:
    """Read ``geo.avalanche_org`` and append its converted rows to ``geo.avalanche_org_etl``.

    Delegates to :func:`utils.etl_utils.run_etl` with this module's
    :func:`_convert`; see there for the load-type contract.

    Args:
        spark: Active Spark session.
        config: Application configuration (DB schema, JDBC driver, logging).
        db_config: Supabase connection credentials.
        load_type: Load type forced by the caller, or ``None`` to detect it
            from the target table.
        logger: Optional logger; defaults to the runner's logger.

    Returns:
        A human-readable summary, e.g. ``"HISTORICAL: wrote to geo.avalanche_org_etl"``
        or ``"INCREMENTAL: nothing to do — avalanche_org_etl is up to date"``.
    """
    return run_etl(
        spark,
        config,
        db_config,
        SOURCE_TABLE,
        TARGET_TABLE,
        _convert,
        load_type=load_type,
        logger=logger,
    )
