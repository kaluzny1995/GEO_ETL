"""USGS NWIS streamflow → ``geo.usgs_nwis_etl`` (canonical event structure).

Reads the typed plain table ``geo.usgs_nwis`` and writes every gauge
reading into ``geo.usgs_nwis_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl``
tables. The event name and place are parsed from ``site_name`` around its
first ``NEAR``/``AT`` marker on the driver-free path; the gauge point's
``state_province`` is reverse-geocoded through the shared
``cache/reverse_geocode.csv`` and embedded as literal map columns,
because the Spark containers only mount ``GEO_Download/data``.

Run::

    python run_etl.py SINGLE_ETL usgs_nwis_etl
    python run_etl.py SINGLE_ETL usgs_nwis_etl --load-type historical
    python run_etl.py SINGLE_ETL usgs_nwis_etl --load-type incremental

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

SOURCE_TABLE = "usgs_nwis"
TARGET_TABLE = "usgs_nwis_etl"
FILE_DATE_COL = "file_date"
SOURCE_NAME = "USGS"
EVENT_CATEGORY = "flood"
METRIC_NAME = "discharge"
COUNTRY_CODE = "US"
EVENT_NAME_PREFIX = "Streamflow - "
EVENT_NAME_FALLBACK = "Streamflow"
NAME_SPLIT_PATTERN = r"(?i)\s+(?:NEAR|AT)\s+"
QUALIFIER_NAMES = {
    "A": "approved for publication",
    "P": "provisional",
    "e": "estimated",
    "R": "revised",
    "p": "partial record",
    "Bkw": "backwater",
    "Eqp": "equipment malfunction",
    "Rat": "rating being developed",
    "Ssn": "monitored seasonally",
    "ZFL": "zero flow",
    "***": "value unavailable",
}
GEOCODE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache", "reverse_geocode.csv")


def _qualifier_map() -> Column:
    """Literal ``qualifier code → full name`` map column built from :data:`QUALIFIER_NAMES`."""
    return F.create_map(*[F.lit(item) for pair in QUALIFIER_NAMES.items() for item in pair])


def _sentence_case(column: Column) -> Column:
    """Lower-case *column* and upper-case only its first letter.

    Args:
        column: The raw (upper-case source) string column.

    Returns:
        ``POTOMAC RIVER`` → ``Potomac river``; a blank or ``NULL`` input
        stays blank.
    """
    lowered = F.lower(F.trim(column))
    return F.concat(F.upper(F.substring(lowered, 1, 1)), F.substring(lowered, 2, 100000))


def _name_parts() -> tuple[Column, Column]:
    """Split ``site_name`` at its first ``NEAR``/``AT`` marker.

    Returns:
        ``(river_part, place_part)``: the feature name before the marker
        and the locality after it (``NULL`` when there is no marker);
        both are empty strings for a missing ``site_name``.
    """
    parts = F.split(F.coalesce(F.trim(F.col("site_name")), F.lit("")), NAME_SPLIT_PATTERN, 2)
    return parts[0], parts[1]


def _event_name(river: Column) -> Column:
    """``"Streamflow - <sentence-cased river name>"``; ``"Streamflow"`` when blank.

    Args:
        river: Feature part from :func:`_name_parts`.

    Returns:
        The prefixed sentence-cased name, or :data:`EVENT_NAME_FALLBACK`
        when ``site_name`` is missing or blank.
    """
    named = F.when(F.trim(river) != "", F.concat(F.lit(EVENT_NAME_PREFIX), _sentence_case(river)))
    return F.coalesce(named, F.lit(EVENT_NAME_FALLBACK))


def _place_location(river: Column, place: Column) -> Column:
    """``"<sentence-cased river>, <place text>"``; only the river without a marker.

    Args:
        river: Feature part from :func:`_name_parts`.
        place: Locality part from :func:`_name_parts` (``NULL`` without a marker).

    Returns:
        Both parts joined with a comma (``Potomac river, WASH, DC LITTLE
        FALLS PUMP STA``), the sentence-cased feature name alone when the
        name carries no marker, ``NULL`` when ``site_name`` is missing.
    """
    blank_river = F.trim(river) == ""
    place = F.when(place.isNotNull() & (F.trim(place) != ""), F.trim(place))
    return (
        F.when(
            blank_river,
            F.lit(None).cast("string"),
        )
        .when(place.isNotNull(), F.concat_ws(", ", _sentence_case(river), place))
        .otherwise(_sentence_case(river))
    )


def _event_description() -> Column:
    """``qualifiers`` expanded to full names (``provisional``), ``NULL`` when blank."""
    codes = F.transform(F.split(F.coalesce(F.col("qualifiers"), F.lit("")), ","), lambda c: F.trim(c))
    full_names = F.array_join(F.transform(codes, lambda c: F.coalesce(_qualifier_map()[c], c)), ", ")
    return F.when(F.trim(F.coalesce(F.col("qualifiers"), F.lit(""))) != "", full_names).otherwise(
        F.lit(None).cast("string")
    )


def _geocode_map(df: DataFrame, logger: logging.Logger | None = None) -> dict[tuple[float, float], tuple[str, str]]:
    """Resolve every gauge point of *df* to a ``(country, state)`` pair.

    The distinct coordinates of *df* are collected on the driver and handed
    to :func:`utils.reverse_geocode.geocode_map`, which answers them from
    :data:`GEOCODE_FILE` and queries the API only for coordinates the cache
    does not cover yet. The answers are embedded as column literals in
    :func:`_geocode_state`, because the Spark containers only mount
    ``GEO_Download/data``.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to the fetch messages.

    Returns:
        ``(latitude, longitude) -> (country_code, state_province)`` for
        every coordinate of *df*; both parts are empty strings where the
        API found nothing (the ETL keeps ``NULL`` for the state).

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


def _geocode_state(df: DataFrame, logger: logging.Logger | None = None) -> Column:
    """Coordinate → state lookup embedded as a literal map keyed by ``struct(lat, lon)``.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to :func:`_geocode_map`.

    Returns:
        The ``state_province`` of the gauge's coordinate; a coordinate
        missing from the lookup resolves to ``NULL``.
    """
    entries: list[Column] = []
    for (latitude, longitude), (_country, state) in _geocode_map(df, logger).items():
        key = F.struct(F.lit(latitude).alias("latitude"), F.lit(longitude).alias("longitude"))
        entries.extend((key, F.lit(state)))
    if not entries:
        return F.lit(None).cast("string")
    lookup = F.struct(F.col("latitude"), F.col("longitude"))
    return F.create_map(*entries)[lookup]


def _state_province(geocode_state: Column) -> Column:
    """The reverse-geocoded subdivision of the gauge point, ``NULL`` when empty.

    Args:
        geocode_state: ``state_province`` of the coordinate lookup.

    Returns:
        The geocode answer; an empty string (no subdivision found) or a
        missing coordinate resolves to ``NULL``.
    """
    return F.when(geocode_state != "", geocode_state).otherwise(F.lit(None).cast("string"))


def _convert(df: DataFrame, logger: logging.Logger | None = None) -> DataFrame:
    """Map ``geo.usgs_nwis`` columns onto the canonical ETL structure.

    One row per gauge per measurement day per snapshot: the gauge's
    identity (site, name, point) describes the event while ``date``
    places the daily mean in time. The country is always ``US``; the
    subdivision comes from the reverse-geocoded gauge point.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to the lookup fetches.

    Returns:
        The DataFrame in :data:`TARGET_TABLE` column order.
    """
    logger = logger or logging.getLogger(__name__)
    geocode_state = _geocode_state(df, logger)
    river, place = _name_parts()

    return df.select(
        F.lit(SOURCE_NAME).alias("source_name"),
        F.col("site_id").alias("subsource_name"),
        _event_name(river).alias("event_name"),
        F.lit(EVENT_CATEGORY).alias("event_category"),
        F.lit(None).cast("string").alias("event_subcategory"),
        _event_description().alias("event_description"),
        F.col("latitude"),
        F.col("longitude"),
        F.lit(COUNTRY_CODE).alias("country_code"),
        _state_province(geocode_state).alias("state_province"),
        _place_location(river, place).alias("place_location"),
        F.lit(METRIC_NAME).alias("metric_name"),
        F.coalesce(F.col("value"), F.lit(0.0)).alias("metric_value"),
        F.col("unit").alias("metric_unit"),
        F.lit(None).cast("string").alias("danger_rating"),
        F.lit(None).cast("string").alias("statement"),
        F.lit(None).cast("string").alias("other_metrics"),
        F.col("date").cast("date").alias("event_start_date"),
        F.col("date").cast("timestamp").alias("event_start"),
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
    """Read ``geo.usgs_nwis``, transform it and append to ``geo.usgs_nwis_etl``.

    Args:
        spark: Active Spark session.
        config: Application configuration (DB schema, JDBC driver, logging).
        db_config: Supabase connection credentials.
        load_type: Load type forced by the caller, or ``None`` to detect it
            from the target table.
        logger: Optional logger; defaults to the module logger.

    Returns:
        A human-readable summary, e.g. ``"HISTORICAL: wrote to geo.usgs_nwis_etl"``
        or ``"INCREMENTAL: nothing to do — usgs_nwis_etl is up to date"``.
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
