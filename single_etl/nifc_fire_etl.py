"""NIFC → ``geo.nifc_fire_etl`` (canonical event structure).

Reads the typed plain table ``geo.nifc_fire`` and writes every fire
perimeter snapshot into ``geo.nifc_fire_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl``
tables. The jurisdiction's full name ships with every source row
(``unit_name``, resolved by GEO_Download through its consolidated
``data/nifc_unit_names.csv`` lookup when the daily CSVs are written), so
this ETL performs no unit lookup and needs no network for it. The one
remaining lookup runs on the driver and is embedded as literal map
columns, because the Spark containers only mount ``GEO_Download/data``:

- the ``state`` of the pre-state vintage comes from the shared
  ``cache/reverse_geocode.csv`` — only rows with a blank ``state`` are
  reverse-geocoded, the source's own subdivision always wins.

Run::

    python run_etl.py SINGLE_ETL nifc_fire_etl
    python run_etl.py SINGLE_ETL nifc_fire_etl --load-type historical
    python run_etl.py SINGLE_ETL nifc_fire_etl --load-type incremental

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

SOURCE_TABLE = "nifc_fire"
TARGET_TABLE = "nifc_fire_etl"
FILE_DATE_COL = "file_date"
SOURCE_NAME = "NIFC"
SUBSOURCE_NAME = "WFIGS"
COUNTRY_CODE = "US"
EVENT_CATEGORY = "fire"
METRIC_NAME = "area"
METRIC_UNIT = "acres"
PERCENT_UNIT = "%"
FIRE_TYPE_MAP = {
    "WF": "wildfire",
    "RX": "prescribed fire",
}
GEOCODE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache", "reverse_geocode.csv")


def _needs_geocode() -> Column:
    """Rows whose ``state`` is blank — only those coordinates are looked up.

    Returns:
        ``TRUE`` when ``state`` is ``NULL`` or trims to an empty string.
    """
    state = F.trim(F.col("state"))
    return state.isNull() | (state == "")


def _geocode_map(df: DataFrame, logger: logging.Logger | None = None) -> dict[tuple[float, float], str]:
    """Resolve every stateless coordinate of *df* to a subdivision code.

    The distinct coordinates of rows with a blank ``state`` are collected on
    the driver and handed to :func:`utils.reverse_geocode.geocode_map`, which
    answers them from :data:`GEOCODE_FILE` and queries the API only for
    coordinates the cache does not cover yet. The answers are embedded as
    column literals in :func:`_state_geocode_column`, because the Spark
    containers only mount ``GEO_Download/data``.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to the fetch messages.

    Returns:
        ``(latitude, longitude) -> state_province`` for every stateless
        coordinate of *df*; the state is ``""`` where the API found none
        and the ETL keeps ``NULL``.

    Raises:
        RuntimeError: When new coordinates could not be fetched (no network).
    """
    points = {
        (row["latitude"], row["longitude"])
        for row in df.filter(_needs_geocode()).select("latitude", "longitude").distinct().collect()
        if row["latitude"] is not None and row["longitude"] is not None
    }
    cache = geocode_map(points, output=GEOCODE_FILE, logger=logger)
    return {point: cache[coord_key(*point)][1] for point in points}


def _state_geocode_column(df: DataFrame, logger: logging.Logger | None = None) -> Column:
    """Coordinate lookup embedded as a literal map keyed by ``struct(lat, lon)``.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to :func:`_geocode_map`.

    Returns:
        The geocoded ``state_province``; a coordinate missing from the lookup
        resolves to ``NULL`` and the ETL keeps its own ``NULL``.
    """
    entries: list[Column] = []
    for (latitude, longitude), state in _geocode_map(df, logger).items():
        entries.append(F.struct(F.lit(latitude).alias("latitude"), F.lit(longitude).alias("longitude")))
        entries.append(F.lit(state))
    if not entries:
        return F.lit(None).cast("string")
    geocode = F.create_map(*entries)[F.struct(F.col("latitude"), F.col("longitude"))]
    return geocode


def _event_subcategory() -> Column:
    """``event_subcategory`` from ``fire_type`` (``FIRE_TYPE_MAP``), ``NULL`` when blank or unknown."""
    known = F.create_map(*[F.lit(item) for pair in FIRE_TYPE_MAP.items() for item in pair])
    return known[F.trim(F.col("fire_type"))]


def _state_province(geocode_state: Column) -> Column:
    """The source's subdivision, else the geocoded one, ``NULL`` when neither resolves.

    Args:
        geocode_state: ``state_province`` of the coordinate lookup.

    Returns:
        The trimmed source ``state`` (the field exists from the 2026-09-18
        vintage on), otherwise the reverse-geocoded answer, otherwise
        ``NULL`` (no coordinate).
    """
    source = F.trim(F.col("state"))
    return F.when(source != "", source).when(geocode_state != "", geocode_state).otherwise(F.lit(None).cast("string"))


def _place_location() -> Column:
    """The jurisdiction unit's full name as it ships with the source row.

    Returns:
        The trimmed ``unit_name`` that GEO_Download resolved from
        ``jurisdiction_label`` at download time (a code the lookup does
        not know keeps its raw value); a blank or ``NULL`` value — no
        unit code — maps to ``NULL``.
    """
    trimmed = F.trim(F.col("unit_name"))
    return F.when(trimmed != "", trimmed).otherwise(F.lit(None).cast("string"))


def _event_name() -> Column:
    """``incident_name``, ``NULL`` when blank."""
    trimmed = F.trim(F.col("incident_name"))
    return F.when(trimmed != "", trimmed).otherwise(F.lit(None).cast("string"))


def _other_metrics() -> Column:
    """The containment of the fire as a JSON object string.

    Returns:
        ``{"percent_contained": …, "percent_contained_unit": "%"}`` — always
        present: a missing ``percent_contained`` counts as ``0`` (nothing
        reported yet), so both keys and the ``%`` companion stay valid.
    """
    percent = F.round(F.coalesce(F.col("percent_contained"), F.lit(0.0))).cast("long")
    return F.to_json(F.struct(percent.alias("percent_contained"), F.lit(PERCENT_UNIT).alias("percent_contained_unit")))


def _convert(df: DataFrame, logger: logging.Logger | None = None) -> DataFrame:
    """Map ``geo.nifc_fire`` columns onto the canonical ETL structure.

    One row per perimeter snapshot: the fire's identity (name, type,
    jurisdiction, acres, containment) describes the perimeter while the
    centroid and ``created_on`` place it in space and time.
    The source's ``state`` wins; the pre-state vintage resolves from the
    reverse-geocoded centroid. ``unit_name`` ships with the row, so no
    unit lookup runs here.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to :func:`_state_geocode_column`.

    Returns:
        The DataFrame in :data:`TARGET_TABLE` column order.
    """
    logger = logger or logging.getLogger(__name__)
    geocode_state = _state_geocode_column(df, logger)
    place_location = _place_location()

    return df.select(
        F.lit(SOURCE_NAME).alias("source_name"),
        F.lit(SUBSOURCE_NAME).alias("subsource_name"),
        _event_name().alias("event_name"),
        F.lit(EVENT_CATEGORY).alias("event_category"),
        _event_subcategory().alias("event_subcategory"),
        F.lit(None).cast("string").alias("event_description"),
        F.col("latitude"),
        F.col("longitude"),
        F.lit(COUNTRY_CODE).alias("country_code"),
        _state_province(geocode_state).alias("state_province"),
        place_location.alias("place_location"),
        F.lit(METRIC_NAME).alias("metric_name"),
        F.coalesce(F.col("acres"), F.lit(0.0)).alias("metric_value"),
        F.lit(METRIC_UNIT).alias("metric_unit"),
        F.lit(None).cast("string").alias("danger_rating"),
        _other_metrics().alias("other_metrics"),
        F.col("created_on").alias("event_start"),
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
    """Read ``geo.nifc_fire``, transform it and append to ``geo.nifc_fire_etl``.

    Args:
        spark: Active Spark session.
        config: Application configuration (DB schema, JDBC driver, logging).
        db_config: Supabase connection credentials.
        load_type: Load type forced by the caller, or ``None`` to detect it
            from the target table.
        logger: Optional logger; defaults to the module logger.

    Returns:
        A human-readable summary, e.g. ``"HISTORICAL: wrote to geo.nifc_fire_etl"``
        or ``"INCREMENTAL: nothing to do — nifc_fire_etl is up to date"``.
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
