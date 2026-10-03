"""USGS volcanic activity notices → ``geo.usgs_volcano_etl`` (canonical event structure).

Reads the typed plain table ``geo.usgs_volcano`` and writes every notice row
into ``geo.usgs_volcano_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl``
tables. The notice details (coordinates, region, status line and full notice
type) are already stamped into the source CSVs by the downloader in
GEO_Download and arrive through the plain table, so this ETL only performs
one driver-side lookup: the ``state_province`` of the volcano point comes
from the shared ``cache/reverse_geocode.csv`` (``ZZ`` when the geocode has
no subdivision, e.g. the Ahyi Seamount ocean point). The geocode is strict —
a failed request raises and fails the build instead of leaving holes, while
successful answers are persisted for every later run.

Run::

    python run_etl.py SINGLE_ETL usgs_volcano_etl
    python run_etl.py SINGLE_ETL usgs_volcano_etl --load-type historical
    python run_etl.py SINGLE_ETL usgs_volcano_etl --load-type incremental

**Load types** (resolved by :func:`_resolve_load_type`)

- ``HISTORICAL`` — every row of ``SOURCE_TABLE`` is processed (first run, or a
  full reprocess forced with ``--load-type historical``; clear the target first
  if the write must not duplicate rows).
- ``INCREMENTAL`` — only rows whose ``file_date`` is newer than the latest
  ``file_date`` stored in ``TARGET_TABLE``.

``--load-type auto`` (the default) leaves the decision to the script: an empty
target (or a target table that does not exist yet) means ``HISTORICAL``,
otherwise ``INCREMENTAL``. Both types append to the target.
"""

import datetime as dt
import logging
import os

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import LoadType
from utils.raw2plain_utils import get_max_file_date
from utils.reverse_geocode import coord_key, geocode_map

SOURCE_TABLE = "usgs_volcano"
TARGET_TABLE = "usgs_volcano_etl"
FILE_DATE_COL = "file_date"
SOURCE_NAME = "USGS"
EVENT_CATEGORY = "volcano"
METRIC_NAME = "alert_level"
COUNTRY_CODE = "US"
EVENT_NAME_PREFIX = "Volcano - "
EVENT_NAME_FALLBACK = "Volcano"
UNKNOWN_CODE = "ZZ"
ALERT_SCALE = {
    "NORMAL": 0.0,
    "ADVISORY": 1.0,
    "WATCH": 2.0,
    "WARNING": 3.0,
}
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
GEOCODE_FILE = os.path.join(_PROJECT_ROOT, "cache", "reverse_geocode.csv")


def _state_map_column(df: DataFrame, logger: logging.Logger | None = None) -> Column:
    """Reverse-geocoded subdivision embedded as a literal map keyed by ``struct(lat, lon)``.

    The distinct coordinates of *df* are collected on the driver and handed
    to :func:`utils.reverse_geocode.geocode_map`, which answers them from
    :data:`GEOCODE_FILE` and queries the API only for coordinates the cache
    does not cover yet. The answers are embedded as column literals,
    because the Spark containers only mount ``GEO_Download/data``.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to the fetch messages.

    Returns:
        ``state_province`` map column keyed by the row's
        ``(latitude, longitude)`` struct; a point missing from the lookup
        (or a row without coordinates) resolves to ``NULL`` and the
        downstream :func:`_state_province` fallback answers ``ZZ``.
    """
    points = {
        (row["latitude"], row["longitude"])
        for row in df.select("latitude", "longitude").distinct().collect()
        if row["latitude"] is not None and row["longitude"] is not None
    }
    geos = geocode_map(points, output=GEOCODE_FILE, logger=logger)
    entries: list[Column] = []
    for point in points:
        key = F.struct(F.lit(point[0]).alias("latitude"), F.lit(point[1]).alias("longitude"))
        state = geos.get(coord_key(*point), ("", ""))[1]
        entries.extend((key, F.lit(state)))
    if not entries:
        return F.lit(None).cast("string")
    return F.create_map(*entries)[F.struct(F.col("latitude"), F.col("longitude"))]


def _event_name() -> Column:
    """``"Volcano - <volcano_name>"``; ``"Volcano"`` when the name is blank."""
    name = F.trim(F.col("volcano_name"))
    named = F.when(name != "", F.concat(F.lit(EVENT_NAME_PREFIX), name))
    return F.coalesce(named, F.lit(EVENT_NAME_FALLBACK))


def _event_subcategory() -> Column:
    """The full notice type stamped by the downloader, falling back to the feed's own code.

    Returns:
        ``Daily Update``, ``Weekly Update``, ``Volcano Activity
        Notification`` … from ``notice_type_name``; the source
        ``notice_type`` column (``DU``, ``WU``, ``VV``) answers when the
        stamped name is missing, and a blank in both is ``NULL``.
    """
    name = F.trim(F.col("notice_type_name"))
    return F.coalesce(F.when(name != "", name), F.trim(F.col("notice_type")))


def _place_location(region: Column) -> Column:
    """``"<volcano_name>, <region>"``; without a region only the name remains.

    Args:
        region: ``region`` column stamped by the downloader (``Aleutians``).

    Returns:
        Both parts joined with a comma, the name alone when the region is
        missing, ``NULL`` when the volcano name is blank.
    """
    name = F.trim(F.col("volcano_name"))
    region = F.when(region != "", region)
    return F.when(
        name != "",
        F.when(region != "", F.concat_ws(", ", name, region)).otherwise(name),
    ).otherwise(F.lit(None).cast("string"))


def _metric_value() -> Column:
    """The USGS alert scale as a number; an unknown code stays ``NULL``.

    Returns:
        ``NORMAL`` → ``0.0``, ``ADVISORY`` → ``1.0``, ``WATCH`` → ``2.0``,
        ``WARNING`` → ``3.0`` (case-insensitive); ``NULL`` for a blank or
        unrecognised level — an unknown level must not read as ``NORMAL``.
    """
    scale = F.create_map(*[F.lit(item) for pair in ALERT_SCALE.items() for item in pair])
    return scale[F.upper(F.trim(F.coalesce(F.col("alert_level"), F.lit(""))))]


def _state_province(state: Column) -> Column:
    """The reverse-geocoded subdivision, ``ZZ`` when the geocode found none.

    Args:
        state: ``state_province`` of the coordinate lookup.

    Returns:
        The geocode answer (``AK``, ``HI``, ``MP``); an empty answer (the
        Ahyi Seamount sits in open water), a point missing from the lookup
        or a row without coordinates resolves to :data:`UNKNOWN_CODE`.
    """
    return F.coalesce(F.when(state != "", state), F.lit(UNKNOWN_CODE))


def _description(synopsis: Column) -> Column:
    """The notice's one-line status (``synopsis``), ``NULL`` when blank.

    Args:
        synopsis: ``synopsis`` column stamped by the downloader.

    Returns:
        e.g. ``AVO Great Sitkin ORANGE/WATCH - Slow eruption of lava …``;
        a blank line resolves to ``NULL``.
    """
    trimmed = F.trim(synopsis)
    return F.when(trimmed != "", trimmed)


def _convert(df: DataFrame, logger: logging.Logger | None = None) -> DataFrame:
    """Map ``geo.usgs_volcano`` columns onto the canonical ETL structure.

    One row per volcano per notice per snapshot: the feed supplies the
    volcano, observatory, alert level and notice time, the downloader-stamped
    detail columns supply the coordinates, region, status line and full
    notice type — the country is always ``US`` and the subdivision comes
    from the reverse-geocoded volcano point.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to the geocode fetches.

    Returns:
        The DataFrame in :data:`TARGET_TABLE` column order.
    """
    state = _state_map_column(df, logger)

    return df.select(
        F.lit(SOURCE_NAME).alias("source_name"),
        F.col("observatory_name").alias("subsource_name"),
        _event_name().alias("event_name"),
        F.lit(EVENT_CATEGORY).alias("event_category"),
        _event_subcategory().alias("event_subcategory"),
        _description(F.col("synopsis")).alias("event_description"),
        F.col("latitude"),
        F.col("longitude"),
        F.lit(COUNTRY_CODE).alias("country_code"),
        _state_province(state).alias("state_province"),
        _place_location(F.col("region")).alias("place_location"),
        F.lit(METRIC_NAME).alias("metric_name"),
        _metric_value().alias("metric_value"),
        F.lit(None).cast("string").alias("metric_unit"),
        F.col("alert_level").alias("danger_rating"),
        F.lit(None).cast("string").alias("other_metrics"),
        F.col("notice_date").alias("event_start"),
        F.lit(None).cast("timestamp").alias("event_end"),
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
    """Read ``geo.usgs_volcano``, transform it and append to ``geo.usgs_volcano_etl``.

    Args:
        spark: Active Spark session.
        config: Application configuration (DB schema, JDBC driver, logging).
        db_config: Supabase connection credentials.
        load_type: Load type forced by the caller, or ``None`` to detect it
            from the target table.
        logger: Optional logger; defaults to the module logger.

    Returns:
        A human-readable summary, e.g. ``"HISTORICAL: wrote to geo.usgs_volcano_etl"``
        or ``"INCREMENTAL: nothing to do — usgs_volcano_etl is up to date"``.
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
