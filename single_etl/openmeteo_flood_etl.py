"""Open-Meteo flood → ``geo.openmeteo_flood_etl`` (canonical event structure).

Reads the typed plain table ``geo.openmeteo_flood`` and writes every river
discharge snapshot into ``geo.openmeteo_flood_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl``
tables. Two lookups run on the driver and are embedded as literal map
columns, because the Spark containers only mount ``GEO_Download/data``:

- the ISO ``country_code`` comes from the script's own name map over the
  source's ``country`` full names (unknown names fall back to the geocode),
- the reference point's ``state_province`` (and the country fallback) comes
  from the shared ``cache/reverse_geocode.csv`` — every distinct
  ``(latitude, longitude)`` of the plain table is answered there.

Run::

    python run_etl.py SINGLE_ETL openmeteo_flood_etl
    python run_etl.py SINGLE_ETL openmeteo_flood_etl --load-type historical
    python run_etl.py SINGLE_ETL openmeteo_flood_etl --load-type incremental

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

SOURCE_TABLE = "openmeteo_flood"
TARGET_TABLE = "openmeteo_flood_etl"
FILE_DATE_COL = "file_date"
SOURCE_NAME = "OPENMETEO"
SUBSOURCE_NAME = "GLOFAS"
EVENT_CATEGORY = "flood"
METRIC_NAME = "discharge"
METRIC_UNIT = "m³/s"
UNKNOWN_COUNTRY_CODE = "ZZ"
COUNTRY_CODE_MAP = {
    "Afghanistan": "AF",
    "Argentina": "AR",
    "Australia": "AU",
    "Bangladesh": "BD",
    "Botswana": "BW",
    "Brazil": "BR",
    "Cambodia": "KH",
    "Canada": "CA",
    "China": "CN",
    "Colombia": "CO",
    "Costa Rica": "CR",
    "Croatia": "HR",
    "Cuba": "CU",
    "DRC": "CD",
    "Egypt": "EG",
    "France": "FR",
    "Germany": "DE",
    "Ghana": "GH",
    "Haiti": "HT",
    "Hungary": "HU",
    "Iraq": "IQ",
    "Italy": "IT",
    "Jamaica": "JM",
    "Kazakhstan": "KZ",
    "Kenya": "KE",
    "Latvia": "LV",
    "Lithuania": "LT",
    "Mexico": "MX",
    "Mozambique": "MZ",
    "Myanmar": "MM",
    "Nepal": "NP",
    "Netherlands": "NL",
    "Nigeria": "NG",
    "Pakistan": "PK",
    "Papua New Guinea": "PG",
    "Paraguay": "PY",
    "Peru": "PE",
    "Poland": "PL",
    "Portugal": "PT",
    "Romania": "RO",
    "Russia": "RU",
    "Senegal": "SN",
    "Somalia": "SO",
    "Spain": "ES",
    "Sudan": "SD",
    "Tanzania": "TZ",
    "Thailand": "TH",
    "UK": "GB",
    "USA": "US",
    "Ukraine": "UA",
    "Uruguay": "UY",
    "Uzbekistan": "UZ",
    "Venezuela": "VE",
    "Vietnam": "VN",
}
GEOCODE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache", "reverse_geocode.csv")


def _geocode_map(df: DataFrame, logger: logging.Logger | None = None) -> dict[tuple[float, float], tuple[str, str]]:
    """Resolve every reference point of *df* to a ``(country, state)`` pair.

    The distinct coordinates of *df* are collected on the driver and handed
    to :func:`utils.reverse_geocode.geocode_map`, which answers them from
    :data:`GEOCODE_FILE` and queries the API only for coordinates the cache
    does not cover yet. The answers are embedded as column literals in
    :func:`_geocode_columns`, because the Spark containers only mount
    ``GEO_Download/data``.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to the fetch messages.

    Returns:
        ``(latitude, longitude) -> (country_code, state_province)`` for
        every coordinate of *df*; both parts are empty strings where the
        API found nothing (the ETL keeps ``NULL`` and falls back to ``ZZ``).

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
    """Coordinate lookups embedded as literal maps keyed by ``struct(lat, lon)``.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to :func:`_geocode_map`.

    Returns:
        ``(country_column, state_column)``; a coordinate missing from the
        lookup resolves to ``NULL`` in both columns.
    """
    country_entries: list[Column] = []
    state_entries: list[Column] = []
    for (latitude, longitude), (country, state) in _geocode_map(df, logger).items():
        key = F.struct(F.lit(latitude).alias("latitude"), F.lit(longitude).alias("longitude"))
        country_entries.extend((key, F.lit(country)))
        state_entries.extend((key, F.lit(state)))
    if not country_entries:
        return F.lit(None).cast("string"), F.lit(None).cast("string")
    lookup = F.struct(F.col("latitude"), F.col("longitude"))
    return F.create_map(*country_entries)[lookup], F.create_map(*state_entries)[lookup]


def _country_code(geocode_country: Column) -> Column:
    """The ISO country code: source name mapped, else geocoded, else ``ZZ``.

    Args:
        geocode_country: ``country_code`` of the coordinate lookup.

    Returns:
        :data:`COUNTRY_CODE_MAP` applied to the trimmed source ``country``;
        a name the map does not know falls back to the reverse-geocoded
        country of the reference point, and an empty geocode counts as a
        miss (``UNKNOWN_COUNTRY_CODE``).
    """
    mapping = F.create_map(*[F.lit(item) for pair in COUNTRY_CODE_MAP.items() for item in pair])
    return F.coalesce(
        mapping[F.trim(F.col("country"))],
        F.when(geocode_country != "", geocode_country),
        F.lit(UNKNOWN_COUNTRY_CODE),
    )


def _state_province(geocode_state: Column) -> Column:
    """The reverse-geocoded subdivision of the reference point, ``NULL`` when empty.

    Args:
        geocode_state: ``state_province`` of the coordinate lookup.

    Returns:
        The geocode answer; an empty string (no subdivision found) or a
        missing coordinate resolves to ``NULL``.
    """
    return F.when(geocode_state != "", geocode_state).otherwise(F.lit(None).cast("string"))


def _event_name() -> Column:
    """``"<river_name> discharge"``, ``NULL`` when the river name is blank."""
    trimmed = F.trim(F.col("river_name"))
    return F.when(trimmed != "", F.concat(trimmed, F.lit(" discharge"))).otherwise(F.lit(None).cast("string"))


def _place_location() -> Column:
    """``"<river_name> - <country>"``; a blank part is dropped, both blank is ``NULL``."""
    river = F.trim(F.col("river_name"))
    country = F.trim(F.col("country"))
    return F.coalesce(
        F.when((river != "") & (country != ""), F.concat(river, F.lit(" - "), country)),
        F.when(river != "", river),
        F.when(country != "", country),
        F.lit(None).cast("string"),
    )


def _convert(df: DataFrame, logger: logging.Logger | None = None) -> DataFrame:
    """Map ``geo.openmeteo_flood`` columns onto the canonical ETL structure.

    One row per river discharge value snapshot: the river's identity (name,
    country, reference point) describes the event while ``date`` places the
    GloFAS 24-h mean in time. The source's country name wins; unknown names
    fall back to the reverse-geocoded point, which also supplies the
    subdivision.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to the lookup fetches.

    Returns:
        The DataFrame in :data:`TARGET_TABLE` column order.
    """
    logger = logger or logging.getLogger(__name__)
    geocode_country, geocode_state = _geocode_columns(df, logger)

    return df.select(
        F.lit(SOURCE_NAME).alias("source_name"),
        F.lit(SUBSOURCE_NAME).alias("subsource_name"),
        _event_name().alias("event_name"),
        F.lit(EVENT_CATEGORY).alias("event_category"),
        F.lit(None).cast("string").alias("event_subcategory"),
        F.lit(None).cast("string").alias("event_description"),
        F.col("latitude"),
        F.col("longitude"),
        _country_code(geocode_country).alias("country_code"),
        _state_province(geocode_state).alias("state_province"),
        _place_location().alias("place_location"),
        F.lit(METRIC_NAME).alias("metric_name"),
        F.coalesce(F.col("discharge_m3s"), F.lit(0.0)).alias("metric_value"),
        F.lit(METRIC_UNIT).alias("metric_unit"),
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
    """Read ``geo.openmeteo_flood``, transform it and append to ``geo.openmeteo_flood_etl``.

    Args:
        spark: Active Spark session.
        config: Application configuration (DB schema, JDBC driver, logging).
        db_config: Supabase connection credentials.
        load_type: Load type forced by the caller, or ``None`` to detect it
            from the target table.
        logger: Optional logger; defaults to the module logger.

    Returns:
        A human-readable summary, e.g. ``"HISTORICAL: wrote to geo.openmeteo_flood_etl"``
        or ``"INCREMENTAL: nothing to do — openmeteo_flood_etl is up to date"``.
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
