"""NIFC → ``geo.nifc_fire_etl`` (canonical event structure).

Reads the typed plain table ``geo.nifc_fire`` and writes every fire
perimeter snapshot into ``geo.nifc_fire_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl``
tables. The jurisdiction's full name ships with every source row
(``unit_name``, resolved by GEO_Download through its consolidated
``data/nifc_unit_names.csv`` lookup when the daily CSVs are written), so
this ETL performs no unit lookup and needs no network for it. The
subdivision of a perimeter that predates the source's ``state`` field comes
from the ``geocode_state`` column stamped into the CSVs at download time —
only rows with a blank ``state`` carry a stamped value, the source's own
subdivision always wins. No reverse-geocoding API is called here.

Run::

    python run_etl.py SINGLE_ETL nifc_fire_etl
    python run_etl.py SINGLE_ETL nifc_fire_etl --load-type historical
    python run_etl.py SINGLE_ETL nifc_fire_etl --load-type incremental

**Load types** — ``HISTORICAL``, ``INCREMENTAL`` and the ``--load-type auto``
detection are described in :func:`utils.etl_utils.run_etl`.
"""

import logging

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import LoadType
from utils.etl_utils import run_etl

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


def _event_subcategory() -> Column:
    """``event_subcategory`` from ``fire_type`` (``FIRE_TYPE_MAP``), ``NULL`` when blank or unknown."""
    known = F.create_map(*[F.lit(item) for pair in FIRE_TYPE_MAP.items() for item in pair])
    return known[F.trim(F.col("fire_type"))]


def _state_province(geocode_state: Column) -> Column:
    """The source's subdivision, else the stamped one, ``NULL`` when neither resolves.

    Args:
        geocode_state: ``geocode_state`` column stamped into the CSVs at
            download time (only for rows whose source ``state`` is blank).

    Returns:
        The trimmed source ``state`` (the field exists from the 2026-09-18
        vintage on), otherwise the stamped answer, otherwise ``NULL`` (no
        coordinate).
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


def _convert(df: DataFrame) -> DataFrame:
    """Map ``geo.nifc_fire`` columns onto the canonical ETL structure.

    One row per perimeter snapshot: the fire's identity (name, type,
    jurisdiction, acres, containment) describes the perimeter while the
    centroid and ``created_on`` place it in space and time.
    The source's ``state`` wins; the pre-state vintage resolves from the
    stamped ``geocode_state``. ``unit_name`` ships with the row, so no
    unit lookup runs here.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.

    Returns:
        The DataFrame in :data:`TARGET_TABLE` column order.
    """
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
        _state_province(F.col("geocode_state")).alias("state_province"),
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


def run(
    spark: SparkSession,
    config: AppConfig,
    db_config: SupabaseConnectionConfig,
    *,
    load_type: LoadType | None = None,
    logger: logging.Logger | None = None,
) -> str:
    """Read ``geo.nifc_fire`` and append its converted rows to ``geo.nifc_fire_etl``.

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
        A human-readable summary, e.g. ``"HISTORICAL: wrote to geo.nifc_fire_etl"``
        or ``"INCREMENTAL: nothing to do — nifc_fire_etl is up to date"``.
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
