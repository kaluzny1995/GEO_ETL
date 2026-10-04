"""USGS NWIS streamflow → ``geo.usgs_nwis_etl`` (canonical event structure).

Reads the typed plain table ``geo.usgs_nwis`` and writes every gauge
reading into ``geo.usgs_nwis_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl``
tables. The event name and place are parsed from ``site_name`` around its
first ``NEAR``/``AT`` marker on the driver-free path; the gauge point's
``state_province`` is the ``geocode_state`` column stamped into the CSVs at
download time (empty → ``NULL``). No reverse-geocoding API is called here.

Run::

    python run_etl.py SINGLE_ETL usgs_nwis_etl
    python run_etl.py SINGLE_ETL usgs_nwis_etl --load-type historical
    python run_etl.py SINGLE_ETL usgs_nwis_etl --load-type incremental

**Load types** — ``HISTORICAL``, ``INCREMENTAL`` and the ``--load-type auto``
detection are described in :func:`utils.etl_utils.run_etl`.
"""

import logging

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import LoadType
from utils.etl_utils import run_etl

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


def _state_province(geocode_state: Column) -> Column:
    """The stamped subdivision of the gauge point, ``NULL`` when empty.

    Args:
        geocode_state: ``geocode_state`` column stamped into the CSVs at
            download time.

    Returns:
        The stamped answer; an empty string (no subdivision found) or a
        ``NULL`` (no coordinate) resolves to ``NULL``.
    """
    return F.when(geocode_state != "", geocode_state).otherwise(F.lit(None).cast("string"))


def _convert(df: DataFrame) -> DataFrame:
    """Map ``geo.usgs_nwis`` columns onto the canonical ETL structure.

    One row per gauge per measurement day per snapshot: the gauge's
    identity (site, name, point) describes the event while ``date``
    places the daily mean in time. The country is always ``US``; the
    subdivision is the stamped ``geocode_state`` of the gauge point.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.

    Returns:
        The DataFrame in :data:`TARGET_TABLE` column order.
    """
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
        _state_province(F.col("geocode_state")).alias("state_province"),
        _place_location(river, place).alias("place_location"),
        F.lit(METRIC_NAME).alias("metric_name"),
        F.coalesce(F.col("value"), F.lit(0.0)).alias("metric_value"),
        F.col("unit").alias("metric_unit"),
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
    """Read ``geo.usgs_nwis`` and append its converted rows to ``geo.usgs_nwis_etl``.

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
        A human-readable summary, e.g. ``"HISTORICAL: wrote to geo.usgs_nwis_etl"``
        or ``"INCREMENTAL: nothing to do — usgs_nwis_etl is up to date"``.
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
