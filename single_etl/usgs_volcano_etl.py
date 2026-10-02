"""USGS volcanic activity notices → ``geo.usgs_volcano_etl`` (canonical event structure).

Reads the typed plain table ``geo.usgs_volcano`` and writes every notice row
into ``geo.usgs_volcano_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl``
tables. The feed itself carries no location, so two driver-side lookups enrich
each row and are embedded as literal map columns, because the Spark containers
only mount ``GEO_Download/data``:

- the volcano's coordinates, region, status line and full notice type come
  from the notice API behind ``notice_data_url``, keyed by
  ``notice_identifier|vnum`` and cached in
  ``cache/usgs_volcano_notices.csv`` (immutable notices, fetched once),
- the ``state_province`` of that point comes from the shared
  ``cache/reverse_geocode.csv`` (``ZZ`` when the geocode has no
  subdivision, e.g. the Ahyi Seamount ocean point).

Both fetches are strict: a failed request raises and fails the build instead
of leaving holes in the cache, and successful answers are persisted for every
later run.

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
from utils.fetch_volcano_notices import notice_key, notice_map
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
NOTICE_FIELDS = ("notice_type", "latitude", "longitude", "region", "synopsis")
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
NOTICE_FILE = os.path.join(_PROJECT_ROOT, "cache", "usgs_volcano_notices.csv")
GEOCODE_FILE = os.path.join(_PROJECT_ROOT, "cache", "reverse_geocode.csv")


def _notice_keys(df: DataFrame) -> list[tuple[str, str]]:
    """Distinct ``(notice_identifier, vnum)`` pairs of *df*.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.

    Returns:
        One pair per notice/volcano combination; rows with a missing
        identifier or volcano number are skipped (they cannot be enriched
        and fall back to the feed's own fields downstream).
    """
    return [
        (row["notice_identifier"], str(row["vnum"]))
        for row in df.select("notice_identifier", "vnum").distinct().collect()
        if row["notice_identifier"] is not None and row["vnum"] is not None
    ]


def _enrichment_columns(df: DataFrame, logger: logging.Logger | None = None) -> dict[str, Column]:
    """Notice details and geocoded state embedded as literal map columns.

    Every distinct ``notice_identifier|vnum`` of *df* is resolved through
    :func:`utils.fetch_volcano_notices.notice_map` (one API call per new
    notice), and the coordinates found there are reverse-geocoded through
    :func:`utils.reverse_geocode.geocode_map`. Both caches live on the
    driver, so nothing under ``cache/`` has to be reachable from the
    executors.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to the fetches.

    Returns:
        ``field -> map column`` for :data:`NOTICE_FIELDS` plus
        ``state_province``; the map is keyed by the row's
        ``notice_identifier|vnum`` and a pair missing from the lookup
        resolves to ``NULL`` (the source fields answer downstream).
    """
    logger = logger or logging.getLogger(__name__)
    keys = _notice_keys(df)
    notices = notice_map(keys, output=NOTICE_FILE, logger=logger)
    requested = {notice_key(identifier, vnum) for identifier, vnum in keys}
    points = {
        (float(notices[key]["latitude"]), float(notices[key]["longitude"]))
        for key in requested
        if key in notices and notices[key]["latitude"] and notices[key]["longitude"]
    }
    geos = geocode_map(points, output=GEOCODE_FILE, logger=logger)

    values: dict[str, dict[str, str]] = {field: {} for field in (*NOTICE_FIELDS, "state_province")}
    for key in requested:
        notice = notices.get(key)
        if notice is None:
            continue
        for field in NOTICE_FIELDS:
            values[field][key] = notice[field]
        latitude, longitude = notice["latitude"], notice["longitude"]
        state = geos.get(coord_key(latitude, longitude), ("", ""))[1] if latitude and longitude else ""
        values["state_province"][key] = state

    lookup = F.concat_ws("|", F.col("notice_identifier"), F.col("vnum").cast("string"))
    columns: dict[str, Column] = {}
    for field, mapping in values.items():
        if not mapping:
            columns[field] = F.lit(None).cast("string")
            continue
        entries = [item for key, value in mapping.items() for item in (F.lit(key), F.lit(value))]
        columns[field] = F.create_map(*entries)[lookup]
    return columns


def _event_name() -> Column:
    """``"Volcano - <volcano_name>"``; ``"Volcano"`` when the name is blank."""
    name = F.trim(F.col("volcano_name"))
    named = F.when(name != "", F.concat(F.lit(EVENT_NAME_PREFIX), name))
    return F.coalesce(named, F.lit(EVENT_NAME_FALLBACK))


def _event_subcategory(notice_type: Column) -> Column:
    """The API's full notice type, falling back to the feed's own code.

    Args:
        notice_type: ``notice_type`` of the cached notice payload.

    Returns:
        ``Daily Update``, ``Weekly Update``, ``Volcano Activity
        Notification`` …; the source ``notice_type`` column (``DU``,
        ``WU``, ``VV``) answers when the cache carried no name, and a
        blank in both is ``NULL``.
    """
    return F.coalesce(F.when(notice_type != "", notice_type), F.trim(F.col("notice_type")))


def _place_location(region: Column) -> Column:
    """``"<volcano_name>, <region>"``; without a region only the name remains.

    Args:
        region: ``region`` of the cached notice section (``Aleutians``).

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
        Ahyi Seamount sits in open water) or a missing notice entry
        resolves to :data:`UNKNOWN_CODE`.
    """
    return F.coalesce(F.when(state != "", state), F.lit(UNKNOWN_CODE))


def _statement(synopsis: Column) -> Column:
    """The notice's one-line status (``synopsis``), ``NULL`` when blank.

    Args:
        synopsis: ``synopsis`` of the cached notice section.

    Returns:
        e.g. ``AVO Great Sitkin ORANGE/WATCH - Slow eruption of lava …``;
        a blank line resolves to ``NULL``.
    """
    trimmed = F.trim(synopsis)
    return F.when(trimmed != "", trimmed)


def _convert(df: DataFrame, logger: logging.Logger | None = None) -> DataFrame:
    """Map ``geo.usgs_volcano`` columns onto the canonical ETL structure.

    One row per volcano per notice per snapshot: the feed supplies the
    volcano, observatory, alert level and notice time, while the notice
    API adds the coordinates, region, status line and full notice type —
    the country is always ``US`` and the subdivision comes from the
    reverse-geocoded volcano point.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to the lookup fetches.

    Returns:
        The DataFrame in :data:`TARGET_TABLE` column order.
    """
    logger = logger or logging.getLogger(__name__)
    enriched = _enrichment_columns(df, logger)

    return df.select(
        F.lit(SOURCE_NAME).alias("source_name"),
        F.col("observatory_name").alias("subsource_name"),
        _event_name().alias("event_name"),
        F.lit(EVENT_CATEGORY).alias("event_category"),
        _event_subcategory(enriched["notice_type"]).alias("event_subcategory"),
        F.lit(None).cast("string").alias("event_description"),
        F.when(enriched["latitude"] != "", enriched["latitude"].cast("double")).alias("latitude"),
        F.when(enriched["longitude"] != "", enriched["longitude"].cast("double")).alias("longitude"),
        F.lit(COUNTRY_CODE).alias("country_code"),
        _state_province(enriched["state_province"]).alias("state_province"),
        _place_location(enriched["region"]).alias("place_location"),
        F.lit(METRIC_NAME).alias("metric_name"),
        _metric_value().alias("metric_value"),
        F.lit(None).cast("string").alias("metric_unit"),
        F.col("alert_level").alias("danger_rating"),
        _statement(enriched["synopsis"]).alias("statement"),
        F.lit(None).cast("string").alias("other_metrics"),
        F.col("notice_date").cast("date").alias("event_start_date"),
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
