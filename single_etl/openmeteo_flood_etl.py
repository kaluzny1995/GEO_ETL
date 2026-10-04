"""Open-Meteo flood → ``geo.openmeteo_flood_etl`` (canonical event structure).

Reads the typed plain table ``geo.openmeteo_flood`` and writes every river
discharge snapshot into ``geo.openmeteo_flood_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl``
tables. The ISO ``country_code`` comes from the script's own name map over
the source's ``country`` full names (unknown names fall back to the
``geocode_country`` column stamped into the CSVs at download time), and the
reference point's ``state_province`` is the stamped ``geocode_state``
column. No reverse-geocoding API is called here.

Run::

    python run_etl.py SINGLE_ETL openmeteo_flood_etl
    python run_etl.py SINGLE_ETL openmeteo_flood_etl --load-type historical
    python run_etl.py SINGLE_ETL openmeteo_flood_etl --load-type incremental

**Load types** — ``HISTORICAL``, ``INCREMENTAL`` and the ``--load-type auto``
detection are described in :func:`utils.etl_utils.run_etl`.
"""

import logging

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import LoadType
from utils.etl_utils import run_etl

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


def _country_code(geocode_country: Column) -> Column:
    """The ISO country code: source name mapped, else stamped geocode, else ``ZZ``.

    Args:
        geocode_country: ``geocode_country`` column stamped into the CSVs at
            download time.

    Returns:
        :data:`COUNTRY_CODE_MAP` applied to the trimmed source ``country``;
        a name the map does not know falls back to the stamped country of
        the reference point, and an empty stamp counts as a miss
        (``UNKNOWN_COUNTRY_CODE``).
    """
    mapping = F.create_map(*[F.lit(item) for pair in COUNTRY_CODE_MAP.items() for item in pair])
    return F.coalesce(
        mapping[F.trim(F.col("country"))],
        F.when(geocode_country != "", geocode_country),
        F.lit(UNKNOWN_COUNTRY_CODE),
    )


def _state_province(geocode_state: Column) -> Column:
    """The stamped subdivision of the reference point, ``NULL`` when empty.

    Args:
        geocode_state: ``geocode_state`` column stamped into the CSVs at
            download time.

    Returns:
        The stamped answer; an empty string (no subdivision found) or a
        ``NULL`` (no coordinate) resolves to ``NULL``.
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


def _convert(df: DataFrame) -> DataFrame:
    """Map ``geo.openmeteo_flood`` columns onto the canonical ETL structure.

    One row per river discharge value snapshot: the river's identity (name,
    country, reference point) describes the event while ``date`` places the
    GloFAS 24-h mean in time. The source's country name wins; unknown names
    fall back to the stamped ``geocode_country``, which also supplies the
    subdivision through ``geocode_state``.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.

    Returns:
        The DataFrame in :data:`TARGET_TABLE` column order.
    """
    return df.select(
        F.lit(SOURCE_NAME).alias("source_name"),
        F.lit(SUBSOURCE_NAME).alias("subsource_name"),
        _event_name().alias("event_name"),
        F.lit(EVENT_CATEGORY).alias("event_category"),
        F.lit(None).cast("string").alias("event_subcategory"),
        F.lit(None).cast("string").alias("event_description"),
        F.col("latitude"),
        F.col("longitude"),
        _country_code(F.col("geocode_country")).alias("country_code"),
        _state_province(F.col("geocode_state")).alias("state_province"),
        _place_location().alias("place_location"),
        F.lit(METRIC_NAME).alias("metric_name"),
        F.coalesce(F.col("discharge_m3s"), F.lit(0.0)).alias("metric_value"),
        F.lit(METRIC_UNIT).alias("metric_unit"),
        F.lit(None).cast("string").alias("danger_rating"),
        F.lit(None).cast("string").alias("other_metrics"),
        F.col("date").cast("timestamp").alias("event_start"),
        F.lit(None).cast("timestamp").alias("event_end"),
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
    """Read ``geo.openmeteo_flood`` and append its converted rows to ``geo.openmeteo_flood_etl``.

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
        A human-readable summary, e.g. ``"HISTORICAL: wrote to geo.openmeteo_flood_etl"``
        or ``"INCREMENTAL: nothing to do — openmeteo_flood_etl is up to date"``.
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
