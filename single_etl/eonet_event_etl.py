"""NASA EONET → ``geo.eonet_event_etl`` (canonical event structure).

Reads the typed plain table ``geo.eonet_event`` and writes every event into
``geo.eonet_event_etl`` using the schema from ``model/etl/event_etl.py``, so it
can be merged with the other ``*_etl`` tables. Country and state come from the
US state at the end of ``description`` when the source states one, otherwise
from the coordinate lookup ``cache/reverse_geocode.csv``; coordinates missing
from that file are reverse-geocoded through ``utils.reverse_geocode`` on the
driver (the same literal-map pattern as the avalanche zone lookup, because the
Spark containers only mount ``GEO_Download/data``).

Run::

    python run_etl.py SINGLE_ETL eonet_event_etl
    python run_etl.py SINGLE_ETL eonet_event_etl --load-type historical
    python run_etl.py SINGLE_ETL eonet_event_etl --load-type incremental

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

SOURCE_TABLE = "eonet_event"
TARGET_TABLE = "eonet_event_etl"
FILE_DATE_COL = "file_date"
SOURCE_NAME = "EONET"
FALLBACK_COUNTRY = "ZZ"
GEOCODE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache", "reverse_geocode.csv")
CATEGORY_MAP = {
    "severeStorms": "storm",
    "wildfires": "fire",
    "seaLakeIce": "iceberg",
}


def _geocode_map(df: DataFrame, logger: logging.Logger | None = None) -> dict[tuple[float, float], tuple[str, str]]:
    """Resolve every coordinate of *df* to ``(country_code, state_province)``.

    The distinct coordinates are collected on the driver and handed to
    :func:`utils.reverse_geocode.geocode_map`, which answers them from
    :data:`GEOCODE_FILE` and queries the API only for coordinates the cache
    does not cover yet. The answers are embedded as column literals in
    :func:`_geocode_columns`, because the Spark containers only mount
    ``GEO_Download/data``.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to the fetch messages.

    Returns:
        ``(latitude, longitude) -> (country_code, state_province)`` for every
        coordinate of *df*; the country is ``""`` where the API found none
        (open water) and the ETL turns that into :data:`FALLBACK_COUNTRY`.

    Raises:
        RuntimeError: When new coordinates could not be fetched (no network).
    """
    points = {
        (row["latitude"], row["longitude"])
        for row in df.select("latitude", "longitude").distinct().collect()
        if row["latitude"] is not None and row["longitude"] is not None
    }
    cache = geocode_map(points, output=GEOCODE_FILE, logger=logger)
    return {point: cache[coord_key(*point)] for point in points}


def _geocode_columns(df: DataFrame, logger: logging.Logger | None = None) -> tuple[Column, Column]:
    """Coordinate lookup embedded as a literal map keyed by ``struct(lat, lon)``.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to :func:`_geocode_map`.

    Returns:
        ``(country_code, state_province)`` columns; a coordinate missing from
        the lookup resolves to ``NULL`` and the ETL applies its fallback.
    """
    entries: list[Column] = []
    for (latitude, longitude), (country, state) in _geocode_map(df, logger).items():
        entries.append(F.struct(F.lit(latitude).alias("latitude"), F.lit(longitude).alias("longitude")))
        entries.append(F.struct(F.lit(country).alias("country"), F.lit(state).alias("state")))
    if not entries:
        return F.lit(None).cast("string"), F.lit(None).cast("string")
    geocode = F.create_map(*entries)[F.struct(F.col("latitude"), F.col("longitude"))]
    return geocode["country"], geocode["state"]


def _state_from_description() -> Column:
    """The 2-letter US state at the end of ``description`` (``…, MT``), else ``""``."""
    return F.regexp_extract(F.coalesce(F.trim(F.col("description")), F.lit("")), r",\s*([A-Z]{2})\s*$", 1)


def _country_code(description_state: Column, geocode_country: Column) -> Column:
    """US when the description names a state, else the geocoded country, else ``ZZ``.

    Args:
        description_state: Result of :func:`_state_from_description`.
        geocode_country: ``country_code`` of the coordinate lookup.

    Returns:
        The row's ISO 3166-1 alpha-2 country code.
    """
    return (
        F.when(description_state != "", F.lit("US"))
        .when(geocode_country != "", geocode_country)
        .otherwise(F.lit(FALLBACK_COUNTRY))
    )


def _state_province(description_state: Column, geocode_state: Column) -> Column:
    """The description's state, else the geocoded subdivision, else ``NULL``.

    Args:
        description_state: Result of :func:`_state_from_description`.
        geocode_state: ``state_province`` of the coordinate lookup.

    Returns:
        The row's subdivision code (``US-MT`` → ``MT``).
    """
    return (
        F.when(description_state != "", description_state)
        .when(geocode_state != "", geocode_state)
        .otherwise(F.lit(None).cast("string"))
    )


def _event_category() -> Column:
    """``event_category`` from ``category_id`` (``CATEGORY_MAP``), snake_cased otherwise."""
    known = F.create_map(*[F.lit(item) for pair in CATEGORY_MAP.items() for item in pair])
    snake_cased = F.lower(F.regexp_replace(F.col("category_id"), r"([a-z0-9])([A-Z])", "$1_$2"))
    return F.coalesce(known[F.col("category_id")], snake_cased)


def _event_subcategory(category: Column) -> Column:
    """Storm type from ``event_name``; the fire and ice defaults from the category.

    Args:
        category: The row's ``event_category`` expression.

    Returns:
        ``hurricane`` / ``typhoon`` / ``tropical storm`` / ``tropical cyclone`` /
        ``cyclone`` for storms, ``wildfire`` for fires, ``sea_lake_ice`` for ice,
        ``NULL`` when no keyword applies.
    """
    name = F.lower(F.trim(F.col("event_name")))
    storm_type = F.regexp_extract(name, r"(hurricane|typhoon|tropical storm|tropical cyclone|cyclone)", 1)
    return (
        F.when(category == "storm", F.when(storm_type != "", storm_type))
        .when(category == "fire", F.lit("wildfire"))
        .when(category == "iceberg", F.lit("sea_lake_ice"))
        .otherwise(F.lit(None).cast("string"))
    )


def _metric_name(category: Column) -> Column:
    """The headline measurement of the category group (``magnitude`` elsewhere).

    Args:
        category: The row's ``event_category`` expression.

    Returns:
        ``wind speed`` (storms), ``area`` (fires), ``surface`` (ice) or
        ``magnitude`` for any other category.
    """
    return (
        F.when(category == "storm", F.lit("wind speed"))
        .when(category == "fire", F.lit("area"))
        .when(category == "iceberg", F.lit("surface"))
        .otherwise(F.lit("magnitude"))
    )


def _place_location() -> Column:
    """``event_name`` minus its type prefix, when the remainder names a place.

    Fires are named ``Wildfire <incident>, <county>, <state>`` (also
    ``Prescribed Fire``, ``Incident Complex``, ``Emergency Stabilization BAER``),
    so stripping the type prefix leaves the place. Names without a comma
    (``Hurricane Polo``, ``Iceberg D32``) describe the event itself, not a
    place, and become ``NULL``.
    """
    stripped = F.regexp_replace(
        F.trim(F.col("event_name")),
        r"(?i)^(emergency stabilization baer|incident complex|prescribed fire|wildfire)\s+",
        "",
    )
    return F.when(stripped.contains(","), stripped).otherwise(F.lit(None).cast("string"))


def _event_description() -> Column:
    """``description``, ``NULL`` when blank."""
    trimmed = F.trim(F.col("description"))
    return F.when(trimmed != "", trimmed).otherwise(F.lit(None).cast("string"))


def _metric_unit() -> Column:
    """``magnitude_unit``, ``NULL`` when blank."""
    trimmed = F.trim(F.col("magnitude_unit"))
    return F.when(trimmed != "", trimmed).otherwise(F.lit(None).cast("string"))


def _other_metrics() -> Column:
    """``geometry_count`` as a JSON object string (``{"geometry_count": n}``)."""
    return F.when(F.col("geometry_count").isNotNull(), F.to_json(F.struct(F.col("geometry_count"))))


def _convert(df: DataFrame, logger: logging.Logger | None = None) -> DataFrame:
    """Map ``geo.eonet_event`` columns onto the canonical ETL structure.

    One row per event: ``last_latitude``/``last_longitude``/``last_date`` keep the
    latest position and time, ``latitude``/``longitude``/``event_date`` the
    event's own. Country and state are resolved from the description first
    (fires state their US location), then from the reverse-geocoded coordinate;
    storms and ice on the open sea that neither answers get the ``ZZ`` fallback.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to the coordinate lookup fetch.

    Returns:
        The DataFrame in :data:`TARGET_TABLE` column order.
    """
    geocode_country, geocode_state = _geocode_columns(df, logger)
    description_state = _state_from_description()
    category = _event_category()

    return df.select(
        F.lit(SOURCE_NAME).alias("source_name"),
        F.col("source_id").alias("subsource_name"),
        F.col("event_name"),
        category.alias("event_category"),
        _event_subcategory(category).alias("event_subcategory"),
        _event_description().alias("event_description"),
        F.col("latitude"),
        F.col("longitude"),
        _country_code(description_state, geocode_country).alias("country_code"),
        _state_province(description_state, geocode_state).alias("state_province"),
        _place_location().alias("place_location"),
        _metric_name(category).alias("metric_name"),
        F.col("magnitude_value").alias("metric_value"),
        _metric_unit().alias("metric_unit"),
        F.lit(None).cast("string").alias("danger_rating"),
        _other_metrics().alias("other_metrics"),
        F.col("event_date").alias("event_start"),
        F.col("last_date").alias("event_end"),
        F.col("last_latitude"),
        F.col("last_longitude"),
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
    """Read ``geo.eonet_event``, transform it and append to ``geo.eonet_event_etl``.

    Args:
        spark: Active Spark session.
        config: Application configuration (DB schema, JDBC driver, logging).
        db_config: Supabase connection credentials.
        load_type: Load type forced by the caller, or ``None`` to detect it
            from the target table.
        logger: Optional logger; defaults to the module logger.

    Returns:
        A human-readable summary, e.g. ``"INCREMENTAL: wrote to geo.eonet_event_etl"``
        or ``"INCREMENTAL: nothing to do — eonet_event_etl is up to date"``.
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
