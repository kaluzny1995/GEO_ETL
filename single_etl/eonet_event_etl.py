"""NASA EONET → ``geo.eonet_event_etl`` (canonical event structure).

Reads the typed plain table ``geo.eonet_event`` and writes every event into
``geo.eonet_event_etl`` using the schema from ``model/etl/event_etl.py``, so it
can be merged with the other ``*_etl`` tables. Country and state come from the
US state at the end of ``description`` when the source states one, otherwise
from the ``geocode_country``/``geocode_state`` columns stamped into the CSVs
at download time (through GEO_Download's ``data/reverse_geocode.csv``).
No reverse-geocoding API is called here.

Run::

    python run_etl.py SINGLE_ETL eonet_event_etl
    python run_etl.py SINGLE_ETL eonet_event_etl --load-type historical
    python run_etl.py SINGLE_ETL eonet_event_etl --load-type incremental

**Load types** — ``HISTORICAL``, ``INCREMENTAL`` and the ``--load-type auto``
detection are described in :func:`utils.etl_utils.run_etl`.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from model.config.datasets import EonetEventEtlConfig, load_dataset
from model.enums import DangerRating
from single_etl.single_etl_script import SingleEtlScript

_cfg = load_dataset(__file__, "single_etl", EonetEventEtlConfig)


def _state_from_description() -> Column:
    """The 2-letter US state at the end of ``description`` (``…, MT``), else ``""``."""
    return F.regexp_extract(F.coalesce(F.trim(F.col("description")), F.lit("")), r",\s*([A-Z]{2})\s*$", 1)


def _country_code(description_state: Column, geocode_country: Column) -> Column:
    """US when the description names a state, else the stamped country, else ``ZZ``.

    Args:
        description_state: Result of :func:`_state_from_description`.
        geocode_country: ``geocode_country`` column stamped into the CSVs at
            download time.

    Returns:
        The row's ISO 3166-1 alpha-2 country code.
    """
    return (
        F.when(description_state != "", F.lit("US"))
        .when(geocode_country != "", geocode_country)
        .otherwise(F.lit(SingleEtlScript.UNKNOWN_CODE))
    )


def _state_province(description_state: Column, geocode_state: Column) -> Column:
    """The description's state, else the stamped subdivision, else ``NULL``.

    Args:
        description_state: Result of :func:`_state_from_description`.
        geocode_state: ``geocode_state`` column stamped into the CSVs at
            download time.

    Returns:
        The row's subdivision code (``US-MT`` → ``MT``).
    """
    return (
        F.when(description_state != "", description_state)
        .when(geocode_state != "", geocode_state)
        .otherwise(F.lit(None).cast("string"))
    )


def _event_category() -> Column:
    """``event_category`` from ``category_id`` (``_cfg.category_map``), snake_cased otherwise."""
    known = F.create_map(*[F.lit(item) for pair in _cfg.category_map.items() for item in pair])
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


class EonetEventEtlScript(SingleEtlScript):
    """NASA EONET → ``geo.eonet_event_etl`` (canonical event structure)."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_TABLE = _cfg.target_table
    DEDUP_KEYS = ("event_name", "latitude", "longitude", "event_start")

    def _convert(self, df: DataFrame) -> DataFrame:
        """Map ``geo.eonet_event`` columns onto the canonical ETL structure.

        One row per event: ``last_latitude``/``last_longitude``/``last_date`` keep the
        latest position and time, ``latitude``/``longitude``/``event_date`` the
        event's own. Country and state are resolved from the description first
        (fires state their US location), then from the stamped geocode columns;
        storms and ice on the open sea that neither answers get the ``ZZ`` fallback.

        Args:
            df: Rows read from :attr:`SOURCE_TABLE`.

        Returns:
            The DataFrame in :attr:`TARGET_TABLE` column order.
        """
        description_state = _state_from_description()
        category = _event_category()

        return df.select(
            F.lit(_cfg.source_name).alias("source_name"),
            F.col("source_id").alias("subsource_name"),
            F.col("event_name"),
            category.alias("event_category"),
            _event_subcategory(category).alias("event_subcategory"),
            _event_description().alias("event_description"),
            F.col("latitude"),
            F.col("longitude"),
            _country_code(description_state, F.col("geocode_country")).alias("country_code"),
            _state_province(description_state, F.col("geocode_state")).alias("state_province"),
            _place_location().alias("place_location"),
            _metric_name(category).alias("metric_name"),
            F.col("magnitude_value").alias("metric_value"),
            _metric_unit().alias("metric_unit"),
            F.lit(DangerRating.NO_RATING).alias("danger_rating"),
            _other_metrics().alias("other_metrics"),
            F.col("event_date").alias("event_start"),
            F.col("last_date").alias("event_end"),
            F.col("last_latitude"),
            F.col("last_longitude"),
            F.col(self.FILE_DATE_COL),
        )
