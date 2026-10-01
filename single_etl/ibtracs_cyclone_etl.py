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
loads stay correct). The source states no country at all, so every point
within :data:`GEOCODE_DISTANCE_KM` of land is reverse-geocoded through
``utils.reverse_geocode`` on the driver into ``cache/reverse_geocode.csv``
(shared with the EONET and GDACS ETLs; the same literal-map pattern as the
avalanche zone lookup, because the Spark containers only mount
``GEO_Download/data``), and a point the lookup found no country for resolves
to ``ZZ``.

Run::

    python run_etl.py SINGLE_ETL ibtracs_cyclone_etl
    python run_etl.py SINGLE_ETL ibtracs_cyclone_etl --load-type historical
    python run_etl.py SINGLE_ETL ibtracs_cyclone_etl --load-type incremental

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

from pyspark.sql import Column, DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import LoadType
from utils.raw2plain_utils import get_max_file_date
from utils.reverse_geocode import coord_key, geocode_map

SOURCE_TABLE = "ibtracs_cyclone"
TARGET_TABLE = "ibtracs_cyclone_etl"
FILE_DATE_COL = "file_date"
SOURCE_NAME = "IBTRACS"
FALLBACK_COUNTRY = "ZZ"
GEOCODE_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "cache", "reverse_geocode.csv")
GEOCODE_DISTANCE_KM = 100
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


def _geocode_map(df: DataFrame, logger: logging.Logger | None = None) -> dict[tuple[float, float], tuple[str, str]]:
    """Resolve the coordinates of *df* to ``(country_code, state_province)``.

    Only points within :data:`GEOCODE_DISTANCE_KM` of land are fetched — an
    open-ocean coordinate always answers with no country anyway — but every
    cached answer of the remaining points is embedded too, because the shared
    cache may already cover them. The fetch goes through
    :func:`utils.reverse_geocode.geocode_map` on the driver, and the answers
    become column literals in :func:`_geocode_columns` (the Spark containers
    only mount ``GEO_Download/data``).

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to the fetch messages.

    Returns:
        ``(latitude, longitude) -> (country_code, state_province)`` for every
        coordinate of *df*; the country is ``""`` where the lookup found none
        (open water) and the ETL turns that into :data:`FALLBACK_COUNTRY`.

    Raises:
        RuntimeError: When new near-land coordinates could not be fetched.
    """
    points = {
        (row["lat"], row["lon"])
        for row in df.select("lat", "lon").distinct().collect()
        if row["lat"] is not None and row["lon"] is not None
    }
    near_land = {
        (row["lat"], row["lon"])
        for row in df.where(F.col("dist2land") <= F.lit(GEOCODE_DISTANCE_KM)).select("lat", "lon").distinct().collect()
        if row["lat"] is not None and row["lon"] is not None
    }
    cache = geocode_map(near_land, output=GEOCODE_FILE, logger=logger)
    return {point: cache.get(coord_key(*point), ("", "")) for point in points}


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
        entries.append(F.struct(F.lit(latitude).alias("lat"), F.lit(longitude).alias("lon")))
        entries.append(F.struct(F.lit(country).alias("country"), F.lit(state).alias("state")))
    if not entries:
        return F.lit(None).cast("string"), F.lit(None).cast("string")
    geocode = F.create_map(*entries)[F.struct(F.col("lat"), F.col("lon"))]
    return geocode["country"], geocode["state"]


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
    """The reverse-geocoded country of the point, ``ZZ`` when the lookup found none.

    Args:
        geocode_country: ``country_code`` of the coordinate lookup.

    Returns:
        The point's ISO 3166-1 alpha-2 country code (the source carries no
        country) or :data:`FALLBACK_COUNTRY` for open water.
    """
    return F.when(geocode_country != "", geocode_country).otherwise(F.lit(FALLBACK_COUNTRY))


def _state_province(geocode_state: Column) -> Column:
    """The geocoded subdivision, ``NULL`` when the lookup found none.

    Args:
        geocode_state: ``state_province`` of the coordinate lookup.

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


def _convert(df: DataFrame, logger: logging.Logger | None = None) -> DataFrame:
    """Map ``geo.ibtracs_cyclone`` columns onto the canonical ETL structure.

    One row per best-track position: the storm's identity (basin, name,
    intensity) repeats over its points while the coordinate, the wind and
    the dates describe the position itself. Only points within
    :data:`GEOCODE_DISTANCE_KM` of land are reverse-geocoded; everything
    else resolves to ``ZZ`` / ``NULL``.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.
        logger: Logger handed to the coordinate lookup fetch.

    Returns:
        The DataFrame in :data:`TARGET_TABLE` column order.
    """
    geocode_country, geocode_state = _geocode_columns(df, logger)
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
        _country_code(geocode_country).alias("country_code"),
        _state_province(geocode_state).alias("state_province"),
        _place_location().alias("place_location"),
        F.lit("wind speed").alias("metric_name"),
        _metric_value().alias("metric_value"),
        F.lit("kts").alias("metric_unit"),
        _danger_rating().alias("danger_rating"),
        F.lit(None).cast("string").alias("statement"),
        _other_metrics().alias("other_metrics"),
        F.col("iso_time").cast("date").alias("event_start_date"),
        F.col("iso_time").alias("event_start"),
        F.col("iso_time").alias("event_end"),
        F.first("lat").over(latest).alias("last_latitude"),
        F.first("lon").over(latest).alias("last_longitude"),
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
    """Read ``geo.ibtracs_cyclone``, transform it and append to ``geo.ibtracs_cyclone_etl``.

    Args:
        spark: Active Spark session.
        config: Application configuration (DB schema, JDBC driver, logging).
        db_config: Supabase connection credentials.
        load_type: Load type forced by the caller, or ``None`` to detect it
            from the target table.
        logger: Optional logger; defaults to the module logger.

    Returns:
        A human-readable summary, e.g. ``"INCREMENTAL: wrote to geo.ibtracs_cyclone_etl"``
        or ``"INCREMENTAL: nothing to do — ibtracs_cyclone_etl is up to date"``.
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
