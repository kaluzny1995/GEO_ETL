"""GDACS → ``geo.gdacs_event_etl`` (canonical event structure).

Reads the typed plain table ``geo.gdacs_event`` and writes every event
snapshot into ``geo.gdacs_event_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl``
tables. The source states an ISO 3166-1 alpha-3 ``country_code``, which the
script translates to alpha-2 (:data:`COUNTRY_CODE_MAP` — the codes GDACS uses
today; anything the map does not know falls through to the coordinate lookup).
The subdivision comes from ``cache/reverse_geocode.csv`` (shared with the EONET
ETL): every distinct coordinate of the run is reverse-geocoded through
``utils.reverse_geocode`` on the driver (the same literal-map pattern as the
avalanche zone lookup, because the Spark containers only mount
``GEO_Download/data``). Blank and unknown source codes resolve from that
lookup, then fall back to ``ZZ``.

Run::

    python run_etl.py SINGLE_ETL gdacs_event_etl
    python run_etl.py SINGLE_ETL gdacs_event_etl --load-type historical
    python run_etl.py SINGLE_ETL gdacs_event_etl --load-type incremental

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

SOURCE_TABLE = "gdacs_event"
TARGET_TABLE = "gdacs_event_etl"
FILE_DATE_COL = "file_date"
SOURCE_NAME = "GDACS"
FALLBACK_COUNTRY = "ZZ"
GEOCODE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache", "reverse_geocode.csv")
CATEGORY_MAP = {
    "EQ": "earthquake",
    "TC": "storm",
    "FL": "flood",
    "VO": "volcano",
    "WF": "fire",
    "DR": "drought",
}
COUNTRY_CODE_MAP = {
    "AGO": "AO",
    "ALB": "AL",
    "ARG": "AR",
    "AUS": "AU",
    "AUT": "AT",
    "BGD": "BD",
    "BHS": "BS",
    "BIH": "BA",
    "BOL": "BO",
    "BRA": "BR",
    "BTN": "BT",
    "BWA": "BW",
    "CAF": "CF",
    "CAN": "CA",
    "CHL": "CL",
    "CHN": "CN",
    "CMR": "CM",
    "COD": "CD",
    "COG": "CG",
    "COL": "CO",
    "CPV": "CV",
    "CRI": "CR",
    "CYP": "CY",
    "DEU": "DE",
    "DJI": "DJ",
    "DOM": "DO",
    "ECU": "EC",
    "ERI": "ER",
    "ESP": "ES",
    "ETH": "ET",
    "FJI": "FJ",
    "FRA": "FR",
    "GHA": "GH",
    "GIN": "GN",
    "GLP": "GP",
    "GRC": "GR",
    "GTM": "GT",
    "GUM": "GU",
    "HND": "HN",
    "HRV": "HR",
    "HTI": "HT",
    "IDN": "ID",
    "IND": "IN",
    "IRN": "IR",
    "ITA": "IT",
    "JPN": "JP",
    "KAZ": "KZ",
    "KEN": "KE",
    "KHM": "KH",
    "LBY": "LY",
    "LKA": "LK",
    "LTU": "LT",
    "MDG": "MG",
    "MDV": "MV",
    "MEX": "MX",
    "MMR": "MM",
    "MNE": "ME",
    "MNP": "MP",
    "MOZ": "MZ",
    "MYS": "MY",
    "NAM": "NA",
    "NCL": "NC",
    "NGA": "NG",
    "NPL": "NP",
    "NZL": "NZ",
    "PAK": "PK",
    "PAN": "PA",
    "PER": "PE",
    "PHL": "PH",
    "PNG": "PG",
    "POL": "PL",
    "PRI": "PR",
    "PRY": "PY",
    "RUS": "RU",
    "SDN": "SD",
    "SEN": "SN",
    "SLB": "SB",
    "SOM": "SO",
    "SUR": "SR",
    "SVN": "SI",
    "THA": "TH",
    "TJK": "TJ",
    "TON": "TO",
    "TUR": "TR",
    "TWN": "TW",
    "TZA": "TZ",
    "UGA": "UG",
    "UKR": "UA",
    "USA": "US",
    "VEN": "VE",
    "VNM": "VN",
    "VUT": "VU",
    "ZAF": "ZA",
    "ZMB": "ZM",
    "ZWE": "ZW",
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


def _event_category() -> Column:
    """``event_category`` from ``event_type`` (``CATEGORY_MAP``), lowercased otherwise."""
    known = F.create_map(*[F.lit(item) for pair in CATEGORY_MAP.items() for item in pair])
    trimmed = F.trim(F.col("event_type"))
    return F.coalesce(known[trimmed], F.lower(trimmed))


def _event_subcategory(category: Column) -> Column:
    """The cyclone's intensity class, the fire and drought defaults from the text.

    Args:
        category: The row's ``event_category`` expression.

    Returns:
        ``tropical depression`` / ``tropical storm`` / ``hurricane`` for
        cyclones (GDACS's own class in ``severity_text``, ``Hurricane/Typhoon``
        counted as ``hurricane`` to match the EONET keyword), ``wildfire`` for
        fires, ``agricultural drought`` for droughts, ``NULL`` when the source
        states no subtype.
    """
    text = F.lower(F.trim(F.col("severity_text")))
    drought_kind = F.regexp_extract(text, r"for ([a-z]+) drought", 1)
    return (
        F.when(
            category == "storm",
            F.when(text.contains("tropical depression"), F.lit("tropical depression"))
            .when(text.contains("tropical storm"), F.lit("tropical storm"))
            .when(text.contains("hurricane"), F.lit("hurricane")),
        )
        .when(category == "fire", F.lit("wildfire"))
        .when(category == "drought", F.when(drought_kind != "", F.concat_ws(" ", drought_kind, F.lit("drought"))))
        .otherwise(F.lit(None).cast("string"))
    )


def _country_code(geocode_country: Column) -> Column:
    """The source's alpha-3 code translated to alpha-2, else the geocode, else ``ZZ``.

    Args:
        geocode_country: ``country_code`` of the coordinate lookup.

    Returns:
        The row's ISO 3166-1 alpha-2 country code; a code :data:`COUNTRY_CODE_MAP`
        does not know (or a missing one) resolves from the geocoded coordinate.
    """
    alpha2 = F.create_map(*[F.lit(item) for pair in COUNTRY_CODE_MAP.items() for item in pair])
    source_alpha2 = alpha2[F.coalesce(F.upper(F.trim(F.col("country_code"))), F.lit(""))]
    return (
        F.when(source_alpha2.isNotNull(), source_alpha2)
        .when(geocode_country != "", geocode_country)
        .otherwise(F.lit(FALLBACK_COUNTRY))
    )


def _state_province(geocode_state: Column) -> Column:
    """The geocoded subdivision, ``NULL`` when the lookup found none.

    Args:
        geocode_state: ``state_province`` of the coordinate lookup.

    Returns:
        The row's subdivision code (``US-TX`` → ``TX``), or ``NULL``.
    """
    return F.when(geocode_state != "", geocode_state).otherwise(F.lit(None).cast("string"))


def _metric_name(category: Column) -> Column:
    """The headline measurement of the category group (``magnitude`` elsewhere).

    Args:
        category: The row's ``event_category`` expression.

    Returns:
        ``wind speed`` (cyclones), ``area`` (fires and droughts) or
        ``magnitude`` for any other category.
    """
    return (
        F.when(category == "storm", F.lit("wind speed"))
        .when(category.isin("fire", "drought"), F.lit("area"))
        .otherwise(F.lit("magnitude"))
    )


def _metric_value() -> Column:
    """``severity``, with ``0`` ("not defined" for floods and eruptions) mapped to ``NULL``."""
    return F.when(F.col("severity") > 0, F.col("severity"))


def _metric_unit() -> Column:
    """``severity_unit``, ``NULL`` when blank."""
    trimmed = F.trim(F.col("severity_unit"))
    return F.when(trimmed != "", trimmed).otherwise(F.lit(None).cast("string"))


def _place_location() -> Column:
    """``country`` (region at the centroid or affected-country list), ``NULL`` when blank."""
    trimmed = F.trim(F.col("country"))
    return F.when(trimmed != "", trimmed).otherwise(F.lit(None).cast("string"))


def _convert(df: DataFrame, logger: logging.Logger | None = None) -> DataFrame:
    """Map ``geo.gdacs_event`` columns onto the canonical ETL structure.

    One row per event snapshot: the alpha-3 ``country_code`` of the source
    wins, blank and unknown codes resolve from the reverse-geocoded
    coordinate, and the subdivision always comes from that lookup. Floods and
    eruptions define no severity (``severity = 0``), so their metric value and
    unit stay ``NULL``.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to the coordinate lookup fetch.

    Returns:
        The DataFrame in :data:`TARGET_TABLE` column order.
    """
    geocode_country, geocode_state = _geocode_columns(df, logger)
    category = _event_category()

    return df.select(
        F.lit(SOURCE_NAME).alias("source_name"),
        F.col("source").alias("subsource_name"),
        F.col("event_name"),
        category.alias("event_category"),
        _event_subcategory(category).alias("event_subcategory"),
        F.lit(None).cast("string").alias("event_description"),
        F.col("latitude"),
        F.col("longitude"),
        _country_code(geocode_country).alias("country_code"),
        _state_province(geocode_state).alias("state_province"),
        _place_location().alias("place_location"),
        _metric_name(category).alias("metric_name"),
        _metric_value().alias("metric_value"),
        _metric_unit().alias("metric_unit"),
        F.col("alert_level").alias("danger_rating"),
        F.lit(None).cast("string").alias("other_metrics"),
        F.col("from_date").alias("event_start"),
        F.col("to_date").alias("event_end"),
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
    """Read ``geo.gdacs_event``, transform it and append to ``geo.gdacs_event_etl``.

    Args:
        spark: Active Spark session.
        config: Application configuration (DB schema, JDBC driver, logging).
        db_config: Supabase connection credentials.
        load_type: Load type forced by the caller, or ``None`` to detect it
            from the target table.
        logger: Optional logger; defaults to the module logger.

    Returns:
        A human-readable summary, e.g. ``"INCREMENTAL: wrote to geo.gdacs_event_etl"``
        or ``"INCREMENTAL: nothing to do — gdacs_event_etl is up to date"``.
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
