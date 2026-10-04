"""IBTrACS → ``geo.ibtracs_cyclone_etl`` (canonical event structure).

Reads the typed plain table ``geo.ibtracs_cyclone`` and writes every
best-track position into ``geo.ibtracs_cyclone_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl``
tables. One row is one position of one storm: the basin becomes
``subsource_name``, the storm name ``event_name``, the ATCF system status
(``usa_status``) becomes ``event_subcategory`` with the cross-agency
``nature`` as its fallback, and ``last_latitude``/``last_longitude`` carry
the storm's latest position (window maximum of ``iso_time`` per ``sid`` —
the newest points always arrive in the newest ``file_date``, so incremental
loads stay correct). The source states no country at all, so the country
and subdivision come from the ``geocode_country``/``geocode_state`` columns
stamped into the CSVs at download time (GEO_Download reverse-geocodes the
points within 100 km of land through ``data/reverse_geocode.csv`` and stamps
cached answers for the rest too), and a point the stamp found no country
for resolves to ``ZZ``. No reverse-geocoding API is called here.

Run::

    python run_etl.py SINGLE_ETL ibtracs_cyclone_etl
    python run_etl.py SINGLE_ETL ibtracs_cyclone_etl --load-type historical
    python run_etl.py SINGLE_ETL ibtracs_cyclone_etl --load-type incremental

**Load types** — ``HISTORICAL``, ``INCREMENTAL`` and the ``--load-type auto``
detection are described in :func:`utils.etl_utils.run_etl`.
"""

import logging

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import LoadType
from utils.etl_utils import run_etl

SOURCE_TABLE = "ibtracs_cyclone"
TARGET_TABLE = "ibtracs_cyclone_etl"
FILE_DATE_COL = "file_date"
SOURCE_NAME = "IBTRACS"
FALLBACK_COUNTRY = "ZZ"
BASIN_NAMES = {
    "NA": "North Atlantic",
    "EP": "Eastern North Pacific",
    "WP": "Western North Pacific",
    "NI": "North Indian",
    "SI": "South Indian",
    "SP": "South Pacific",
    "SA": "South Atlantic",
}
SUBBASIN_NAMES = {
    "CS": "Caribbean Sea",
    "GM": "Gulf of Mexico",
    "CP": "Central Pacific",
    "AS": "Arabian Sea",
    "BB": "Bay of Bengal",
    "WA": "Western Australia",
    "EA": "Eastern Australia",
}
STATUS_WORDS = {
    "DB": "disturbance",
    "TD": "tropical depression",
    "TS": "tropical storm",
    "TY": "typhoon",
    "ST": "super typhoon",
    "TC": "tropical cyclone",
    "HU": "hurricane",
    "HR": "hurricane",
    "SD": "subtropical depression",
    "SS": "subtropical storm",
    "EX": "extratropical",
    "PT": "post-tropical",
    "IN": "inland",
    "DS": "dissipating",
    "LO": "low",
    "WV": "tropical wave",
    "ET": "extrapolated",
    "MD": "monsoon depression",
    "XX": "unknown",
}
NATURE_WORDS = {
    "DS": "disturbance",
    "TS": "tropical",
    "ET": "extratropical",
    "SS": "subtropical",
    "MX": "contradictory",
}
SSHS_LABELS = {
    "5": "category 5",
    "4": "category 4",
    "3": "category 3",
    "2": "category 2",
    "1": "category 1",
    "0": "tropical storm",
    "-1": "tropical depression",
    "-2": "subtropical",
    "-3": "disturbance",
    "-4": "post-tropical",
    "-5": "unknown",
}


def _event_subcategory() -> Column:
    """The ATCF system status of the point, with the cross-agency ``nature`` as fallback.

    Returns:
        The ``usa_status`` word (:data:`STATUS_WORDS`), else the ``nature``
        word (:data:`NATURE_WORDS`); ``NULL`` when neither reports a type
        (blank status next to ``nature = NR`` in non-US basins).
    """
    status = F.create_map(*[F.lit(item) for pair in STATUS_WORDS.items() for item in pair])
    nature = F.create_map(*[F.lit(item) for pair in NATURE_WORDS.items() for item in pair])
    return F.coalesce(status[F.upper(F.trim(F.col("usa_status")))], nature[F.upper(F.trim(F.col("nature")))])


def _country_code(geocode_country: Column) -> Column:
    """The stamped country of the point, ``ZZ`` when the stamp found none.

    Args:
        geocode_country: ``geocode_country`` column stamped into the CSVs at
            download time.

    Returns:
        The point's ISO 3166-1 alpha-2 country code (the source carries no
        country) or :data:`FALLBACK_COUNTRY` for open water.
    """
    return F.when(geocode_country != "", geocode_country).otherwise(F.lit(FALLBACK_COUNTRY))


def _state_province(geocode_state: Column) -> Column:
    """The stamped subdivision, ``NULL`` when the stamp found none.

    Args:
        geocode_state: ``geocode_state`` column stamped into the CSVs at
            download time.

    Returns:
        The point's subdivision code (``US-TX`` → ``TX``), or ``NULL``.
    """
    return F.when(geocode_state != "", geocode_state).otherwise(F.lit(None).cast("string"))


def _place_location() -> Column:
    """The IBTrACS region name: the full basin name extended with the sub-basin.

    Returns:
        ``North Atlantic - Gulf of Mexico`` / ``Eastern North Pacific -
        Central Pacific`` / … for a named sub-basin (:data:`SUBBASIN_NAMES`);
        just the basin's full name (:data:`BASIN_NAMES`) for the unnamed
        sub-basins (``NA``, ``MM``); ``NULL`` when the basin itself is
        unknown.
    """
    basins = F.create_map(*[F.lit(item) for pair in BASIN_NAMES.items() for item in pair])
    subbasins = F.create_map(*[F.lit(item) for pair in SUBBASIN_NAMES.items() for item in pair])
    basin = basins[F.upper(F.trim(F.col("basin")))]
    subbasin = subbasins[F.upper(F.trim(F.col("subbasin")))]
    return (
        F.when(basin.isNull(), F.lit(None).cast("string"))
        .when(subbasin.isNull(), basin)
        .otherwise(F.concat_ws(" - ", basin, subbasin))
    )


def _metric_value() -> Column:
    """The sustained wind of the point: ``wmo_wind`` when reported, ``usa_wind`` otherwise.

    Returns:
        The wind speed in knots as a double; ``NULL`` when neither agency
        reports one.
    """
    return F.coalesce(F.col("wmo_wind"), F.col("usa_wind")).cast("double")


def _danger_rating() -> Column:
    """The Saffir-Simpson label of ``usa_sshs``.

    Returns:
        ``category 5`` … ``category 1``, ``tropical storm``,
        ``tropical depression``, ``disturbance`` and the remaining classes of
        :data:`SSHS_LABELS`; ``NULL`` when the source reports no class.
    """
    labels = F.create_map(*[F.lit(item) for pair in SSHS_LABELS.items() for item in pair])
    return labels[F.col("usa_sshs").cast("string")]


def _unit_when_present(column: str, unit: str) -> Column:
    """The unit literal for *column*, ``NULL`` wherever the measurement itself is absent.

    Args:
        column: Source column the unit belongs to.
        unit: The unit's name (``mb``, ``kts``, ``degrees``, ``km``).

    Returns:
        *unit* when the row reports *column*, ``NULL`` otherwise — the JSON
        writer drops the ``NULL`` together with the absent measurement.
    """
    return F.when(F.col(column).isNotNull(), F.lit(unit))


def _other_metrics() -> Column:
    """The remaining measurements of the point as a JSON object string.

    Returns:
        ``{"pressure": …, "pressure_unit": …, "speed": …, "speed_unit": …,
        "direction": …, "direction_unit": …, "gust": …, "gust_unit": …,
        "dist2land": …, "dist2land_unit": …, "landfall": …,
        "landfall_unit": …}`` — every measurement paired with its
        ``<name>_unit`` companion key (``mb``, ``kts``, ``degrees``,
        ``km``); measurements the point does not report are omitted
        together with their unit, ``NULL`` when it reports none.
    """
    metrics = F.to_json(
        F.struct(
            F.col("usa_pres").alias("pressure"),
            _unit_when_present("usa_pres", "mb").alias("pressure_unit"),
            F.col("storm_speed").alias("speed"),
            _unit_when_present("storm_speed", "kts").alias("speed_unit"),
            F.col("storm_dir").alias("direction"),
            _unit_when_present("storm_dir", "degrees").alias("direction_unit"),
            F.col("usa_gust").alias("gust"),
            _unit_when_present("usa_gust", "kts").alias("gust_unit"),
            F.col("dist2land").alias("dist2land"),
            _unit_when_present("dist2land", "km").alias("dist2land_unit"),
            F.col("landfall").alias("landfall"),
            _unit_when_present("landfall", "km").alias("landfall_unit"),
        ),
        {"ignoreNullFields": "true"},
    )
    return F.when(metrics != "{}", metrics)


def _convert(df: DataFrame) -> DataFrame:
    """Map ``geo.ibtracs_cyclone`` columns onto the canonical ETL structure.

    One row per best-track position: the storm's identity (basin, name,
    intensity) repeats over its points while the coordinate, the wind and
    the dates describe the position itself. Country and subdivision are the
    stamped geocode columns; points the stamp could not answer resolve to
    ``ZZ`` / ``NULL``.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.

    Returns:
        The DataFrame in :data:`TARGET_TABLE` column order.
    """
    basin = F.trim(F.col("basin"))
    name = F.trim(F.col("name"))
    latest = Window.partitionBy("sid").orderBy(F.col("iso_time").desc())

    return df.select(
        F.lit(SOURCE_NAME).alias("source_name"),
        F.when(basin != "", basin).otherwise(F.lit(None).cast("string")).alias("subsource_name"),
        F.when(name != "", name).otherwise(F.lit(None).cast("string")).alias("event_name"),
        F.lit("storm").alias("event_category"),
        _event_subcategory().alias("event_subcategory"),
        F.lit(None).cast("string").alias("event_description"),
        F.col("lat").alias("latitude"),
        F.col("lon").alias("longitude"),
        _country_code(F.col("geocode_country")).alias("country_code"),
        _state_province(F.col("geocode_state")).alias("state_province"),
        _place_location().alias("place_location"),
        F.lit("wind speed").alias("metric_name"),
        _metric_value().alias("metric_value"),
        F.lit("kts").alias("metric_unit"),
        _danger_rating().alias("danger_rating"),
        _other_metrics().alias("other_metrics"),
        F.col("iso_time").alias("event_start"),
        F.col("iso_time").alias("event_end"),
        F.first("lat").over(latest).alias("last_latitude"),
        F.first("lon").over(latest).alias("last_longitude"),
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
    """Read ``geo.ibtracs_cyclone`` and append its converted rows to ``geo.ibtracs_cyclone_etl``.

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
        A human-readable summary, e.g. ``"HISTORICAL: wrote to geo.ibtracs_cyclone_etl"``
        or ``"INCREMENTAL: nothing to do — ibtracs_cyclone_etl is up to date"``.
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
