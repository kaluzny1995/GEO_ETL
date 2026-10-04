"""GDACS → ``geo.gdacs_event_etl`` (canonical event structure).

Reads the typed plain table ``geo.gdacs_event`` and writes every event
snapshot into ``geo.gdacs_event_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl``
tables. The source states an ISO 3166-1 alpha-3 ``country_code``, which the
script translates to alpha-2 (:data:`COUNTRY_CODE_MAP` — the codes GDACS uses
today; anything the map does not know falls through to the stamped
``geocode_country`` column). The subdivision is the stamped ``geocode_state``
column; both geocode columns are written into the CSVs at download time
(through GEO_Download's ``data/reverse_geocode.csv``). Blank and unknown
source codes resolve from ``geocode_country``, then fall back to ``ZZ``.
No reverse-geocoding API is called here.

Run::

    python run_etl.py SINGLE_ETL gdacs_event_etl
    python run_etl.py SINGLE_ETL gdacs_event_etl --load-type historical
    python run_etl.py SINGLE_ETL gdacs_event_etl --load-type incremental

**Load types** — ``HISTORICAL``, ``INCREMENTAL`` and the ``--load-type auto``
detection are described in :func:`utils.etl_utils.run_etl`.
"""

import logging

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import LoadType
from utils.etl_utils import run_etl

SOURCE_TABLE = "gdacs_event"
TARGET_TABLE = "gdacs_event_etl"
FILE_DATE_COL = "file_date"
SOURCE_NAME = "GDACS"
FALLBACK_COUNTRY = "ZZ"
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
    """The source's alpha-3 code translated to alpha-2, else the stamped country, else ``ZZ``.

    Args:
        geocode_country: ``geocode_country`` column stamped into the CSVs at
            download time.

    Returns:
        The row's ISO 3166-1 alpha-2 country code; a code :data:`COUNTRY_CODE_MAP`
        does not know (or a missing one) resolves from the stamped column.
    """
    alpha2 = F.create_map(*[F.lit(item) for pair in COUNTRY_CODE_MAP.items() for item in pair])
    source_alpha2 = alpha2[F.coalesce(F.upper(F.trim(F.col("country_code"))), F.lit(""))]
    return (
        F.when(source_alpha2.isNotNull(), source_alpha2)
        .when(geocode_country != "", geocode_country)
        .otherwise(F.lit(FALLBACK_COUNTRY))
    )


def _state_province(geocode_state: Column) -> Column:
    """The stamped subdivision, ``NULL`` when the stamp found none.

    Args:
        geocode_state: ``geocode_state`` column stamped into the CSVs at
            download time.

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


def _convert(df: DataFrame) -> DataFrame:
    """Map ``geo.gdacs_event`` columns onto the canonical ETL structure.

    One row per event snapshot: the alpha-3 ``country_code`` of the source
    wins, blank and unknown codes resolve from the stamped
    ``geocode_country``, and the subdivision is the stamped ``geocode_state``.
    Floods and eruptions define no severity (``severity = 0``), so their
    metric value and unit stay ``NULL``.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.

    Returns:
        The DataFrame in :data:`TARGET_TABLE` column order.
    """
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
        _country_code(F.col("geocode_country")).alias("country_code"),
        _state_province(F.col("geocode_state")).alias("state_province"),
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


def run(
    spark: SparkSession,
    config: AppConfig,
    db_config: SupabaseConnectionConfig,
    *,
    load_type: LoadType | None = None,
    logger: logging.Logger | None = None,
) -> str:
    """Read ``geo.gdacs_event`` and append its converted rows to ``geo.gdacs_event_etl``.

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
        A human-readable summary, e.g. ``"HISTORICAL: wrote to geo.gdacs_event_etl"``
        or ``"INCREMENTAL: nothing to do — gdacs_event_etl is up to date"``.
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
