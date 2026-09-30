"""Avalanche.org → ``geo.avalanche_org_etl`` (canonical event structure).

Reads the typed plain table ``geo.avalanche_org`` and writes every forecast
zone/day into ``geo.avalanche_org_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl`` tables.
Zone coordinates come from the centroid lookup
``cache/avalanche_zone_centroids.csv``; when that file is missing the script
builds it itself with ``utils.fetch_avalanche_zones.build_lookup`` (today, 10
and 30 days back).

Run::

    python run_etl.py SINGLE_ETL avalanche_org_etl
    python run_etl.py SINGLE_ETL avalanche_org_etl --load-type historical
    python run_etl.py SINGLE_ETL avalanche_org_etl --load-type incremental

**Load types** (resolved by :func:`_resolve_load_type`)

- ``HISTORICAL`` — every row of ``SOURCE_TABLE`` is processed (first run, or a
  full reprocess forced with ``--load-type historical``; clear the target first
  if the write must not duplicate rows).
- ``INCREMENTAL`` — only rows whose ``file_date`` is newer than the latest
  ``file_date`` stored in ``TARGET_TABLE``.

``--load-type auto`` (the default) leaves the decision to the script: an empty
target (or a target table that does not exist yet) means ``HISTORICAL``,
otherwise ``INCREMENTAL``. Both types append to the target.

**Adding the next source** — copy this file to ``single_etl/<name>_etl.py``,
point ``SOURCE_TABLE``/``TARGET_TABLE`` at its plain and ETL tables, rewrite
:func:`_convert` for its columns, then register it in ``jobs/__init__.py``::

    from single_etl.<name>_etl import run as <name>_etl_run
    SingleEtlFactory.register("<name>_etl", <name>_etl_run)
"""

import csv
import datetime as dt
import logging
import os

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import LoadType
from utils.fetch_avalanche_zones import build_lookup, lookup_days
from utils.raw2plain_utils import get_max_file_date

SOURCE_TABLE = "avalanche_org"
TARGET_TABLE = "avalanche_org_etl"
FILE_DATE_COL = "file_date"
SOURCE_NAME = "AVALANCHE_ORG"
COUNTRY_CODE = "US"
EVENT_CATEGORY = "avalanche"
METRIC_NAME = "magnitude"
ZONE_COORDS_FILE = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache", "avalanche_zone_centroids.csv"
)


def _coords_map(logger: logging.Logger | None = None) -> dict[str, tuple[float, float]]:
    """Parse the zone centroid lookup, building it first when it is missing.

    The CSV is read on the driver: the Spark containers only mount
    ``GEO_Download/data``, so nothing under ``single_etl/`` is reachable from
    the executors. The coordinates are therefore embedded as column literals
    instead of being read as a file (see :func:`_coordinates`).

    A missing file is rebuilt through :func:`utils.fetch_avalanche_zones.build_lookup`
    with the coverage window from :func:`utils.fetch_avalanche_zones.lookup_days`,
    which also creates ``cache/`` when needed.

    Args:
        logger: Logger for the rebuild messages; defaults to the module logger.

    Returns:
        ``"<place_location>|<state>" -> (latitude, longitude)`` for every zone.

    Raises:
        FileNotFoundError: When the lookup is missing and rebuilding it fails.
    """
    if not os.path.exists(ZONE_COORDS_FILE):
        log = logger or logging.getLogger(__name__)
        days = lookup_days()
        log.info(f"Zone centroid lookup missing — building it from avalanche.org for {', '.join(days)}...")
        try:
            written = build_lookup(days=days, output=ZONE_COORDS_FILE)
        except Exception as exc:
            raise FileNotFoundError(
                f"Zone centroid lookup {ZONE_COORDS_FILE} could not be built ({exc}) — "
                "run `python -m utils.fetch_avalanche_zones` manually"
            ) from exc
        log.info(f"Zone centroid lookup built: {written} zones -> {ZONE_COORDS_FILE}")

    with open(ZONE_COORDS_FILE, newline="") as handle:
        return {
            f"{row['place_location'].strip()}|{row['state'].strip()}": (float(row["latitude"]), float(row["longitude"]))
            for row in csv.DictReader(handle)
        }


def _coordinates(logger: logging.Logger | None = None) -> tuple[Column, Column]:
    """Zone centroids embedded as a literal map keyed by ``zone|state``.

    Args:
        logger: Logger handed to :func:`_coords_map`.

    Returns:
        ``(latitude, longitude)`` columns; a zone missing from the lookup
        resolves to ``NULL``.
    """
    entries: list[Column] = []
    for zone, (latitude, longitude) in _coords_map(logger).items():
        entries.append(F.lit(zone))
        entries.append(F.struct(F.lit(latitude).alias("lat"), F.lit(longitude).alias("lon")))
    centroid = F.create_map(*entries)[F.concat_ws("|", F.trim(F.col("zone_name")), F.trim(F.col("state")))]
    return centroid["lat"], centroid["lon"]


def _danger_rating() -> Column:
    """The zone's danger rating, or ``NULL`` while the zone is unrated.

    The source marks unrated zones with ``danger_level = -1`` and a rating text
    of ``no rating``; neither is a rating, so both map to ``NULL``.
    """
    rating = F.trim(F.col("danger_rating"))
    unrated = (F.col("danger_level") <= 0) | (F.lower(rating).isin("", "no rating", "no_rating"))
    return F.when(~unrated, rating)


def _statement() -> Column:
    """Travel advice, with the active avalanche warning appended when present."""
    has_warning = F.col("warning").isNotNull() & (F.trim(F.col("warning")) != "")
    return F.when(has_warning, F.concat_ws(" ", F.col("travel_advice"), F.col("warning"))).otherwise(
        F.col("travel_advice")
    )


def _convert(df: DataFrame, logger: logging.Logger | None = None) -> DataFrame:
    """Map ``geo.avalanche_org`` columns onto the canonical ETL structure.

    Every zone gets its polygon centroid as ``latitude``/``longitude``
    (``NULL`` when the zone is missing from the lookup). A danger level is a
    rating, not a measurement, so ``metric_value`` and ``metric_unit`` stay
    ``NULL`` and unrated zones (``danger_level = -1``) additionally get a
    ``NULL`` ``danger_rating``.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to the centroid lookup build.

    Returns:
        The DataFrame in :data:`TARGET_TABLE` column order.
    """
    latitude, longitude = _coordinates(logger)

    return df.select(
        F.lit(SOURCE_NAME).alias("source_name"),
        F.col("center_name").alias("subsource_name"),
        F.lit(None).cast("string").alias("event_name"),
        F.lit(EVENT_CATEGORY).alias("event_category"),
        F.lit(None).cast("string").alias("event_subcategory"),
        F.lit(None).cast("string").alias("event_description"),
        latitude.alias("latitude"),
        longitude.alias("longitude"),
        F.lit(COUNTRY_CODE).alias("country_code"),
        F.col("state").alias("state_province"),
        F.col("zone_name").alias("place_location"),
        F.lit(METRIC_NAME).alias("metric_name"),
        F.lit(None).cast("double").alias("metric_value"),
        F.lit(None).cast("string").alias("metric_unit"),
        _danger_rating().alias("danger_rating"),
        _statement().alias("statement"),
        F.lit(None).cast("string").alias("other_metrics"),
        F.col("date").cast("date").alias("event_start_date"),
        F.col("forecast_start").alias("event_start"),
        F.col("forecast_end").alias("event_end"),
        F.lit(None).cast("double").alias("last_latitude"),
        F.lit(None).cast("double").alias("last_longitude"),
        F.col(FILE_DATE_COL),
    )


def _resolve_load_type(max_file_date: dt.date | None, requested: LoadType | None) -> LoadType:
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


def run(
    spark: SparkSession,
    config: AppConfig,
    db_config: SupabaseConnectionConfig,
    *,
    load_type: LoadType | None = None,
    logger: logging.Logger | None = None,
) -> str:
    """Read ``geo.avalanche_org``, transform it and append to ``geo.avalanche_org_etl``.

    Args:
        spark: Active Spark session.
        config: Application configuration (DB schema, JDBC driver, logging).
        db_config: Supabase connection credentials.
        load_type: Load type forced by the caller, or ``None`` to detect it
            from the target table.
        logger: Optional logger; defaults to the module logger.

    Returns:
        A human-readable summary, e.g. ``"INCREMENTAL: wrote to geo.avalanche_org_etl"``
        or ``"INCREMENTAL: nothing to do — avalanche_org_etl is up to date"``.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    schema = config.database.db_schema
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(config.database.jdbc_driver)

    logger.info(f"---- Single ETL started: {schema}.{SOURCE_TABLE} → {schema}.{TARGET_TABLE} ----")

    max_file_date = get_max_file_date(spark, jdbc_url, properties, schema, TARGET_TABLE)
    resolved = _resolve_load_type(max_file_date, load_type)

    if load_type is None:
        reason = "target table empty" if max_file_date is None else f"latest file_date in target: {max_file_date}"
        logger.info(f"Load type auto-detected ({reason}) → {resolved.value}")
    elif resolved is not load_type:
        logger.info(f"{load_type.value} requested but the target table is empty → {resolved.value}")
    else:
        logger.info(f"Load type requested: {resolved.value}")

    source_df = spark.read.jdbc(url=jdbc_url, table=f"{schema}.{SOURCE_TABLE}", properties=properties)

    if resolved is LoadType.INCREMENTAL and max_file_date is not None:
        logger.info(f"Processing rows of {SOURCE_TABLE} with {FILE_DATE_COL} > {max_file_date}...")
        source_df = source_df.filter(F.col(FILE_DATE_COL) > F.lit(max_file_date.isoformat()))
    else:
        logger.info(f"Processing every row of {SOURCE_TABLE}...")

    if not source_df.head(1):
        logger.warning(f"No new records for {TARGET_TABLE} — nothing to do, table is up to date")
        return f"{resolved.value}: nothing to do — {TARGET_TABLE} is up to date"

    result_df = _convert(source_df, logger)
    logger.info(f"Writing to {schema}.{TARGET_TABLE}...")
    result_df.write.jdbc(url=jdbc_url, table=f"{schema}.{TARGET_TABLE}", mode="append", properties=properties)
    logger.info(f"Single ETL complete: written to {schema}.{TARGET_TABLE}")
    return f"{resolved.value}: wrote to {schema}.{TARGET_TABLE}"
