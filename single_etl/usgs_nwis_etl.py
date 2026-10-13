"""USGS NWIS streamflow → ``geo.usgs_nwis_etl`` (canonical event structure).

Reads the typed plain table ``geo.usgs_nwis`` and writes every gauge
reading into ``geo.usgs_nwis_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl``
tables. The event name and place are parsed from ``site_name`` around its
first ``NEAR``/``AT`` marker on the driver-free path; the gauge point's
``state_province`` is the ``geocode_state`` column stamped into the CSVs at
download time (empty → ``NULL``). No reverse-geocoding API is called here.

Next to the plain table the script joins the flood-category reference CSV
``data/usgs_flood_categories.csv`` (kept present by GEO_Download's
downloader, one row per gauge with the NWS ``action``/``minor``/``moderate``/
``major`` stage thresholds) on ``site_id``, so a row can say more than "the
discharge was X": ``danger_rating`` carries the NWS category the day's peak
stage reached (``action`` … ``major``, a peak below every threshold stays
``NULL``) and ``other_metrics`` the peak itself against those thresholds.

Run::

    python run_etl.py SINGLE_ETL usgs_nwis_etl
    python run_etl.py SINGLE_ETL usgs_nwis_etl --load-type historical
    python run_etl.py SINGLE_ETL usgs_nwis_etl --load-type incremental

**Load types** — ``HISTORICAL``, ``INCREMENTAL`` and the ``--load-type auto``
detection are described in :func:`utils.etl_utils.run_etl`.
"""

import os

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

from model.config import AppConfig
from model.config.datasets import UsgsNwisEtlConfig, load_dataset
from model.enums import DangerRating
from single_etl.single_etl_script import SingleEtlScript

_cfg = load_dataset(__file__, "single_etl", UsgsNwisEtlConfig)

_FLOOD_CATEGORY_SCHEMA = StructType(
    [
        StructField("site_id", StringType()),
        StructField("identifier", StringType()),
        StructField("site_name", StringType()),
        StructField("state", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        *(StructField(f"{level}_stage_ft", DoubleType()) for level in _cfg.flood_levels),
    ]
)
"""Columns of the flood-category reference, in the order GEO_Download writes them."""


def _qualifier_map() -> Column:
    """Literal ``qualifier code → full name`` map column built from `_cfg.qualifier_names`."""
    return F.create_map(*[F.lit(item) for pair in _cfg.qualifier_names.items() for item in pair])


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
    parts = F.split(F.coalesce(F.trim(F.col("site_name")), F.lit("")), _cfg.name_split_pattern, 2)
    return parts[0], parts[1]


def _event_name(river: Column) -> Column:
    """``"Streamflow - <sentence-cased river name>"``; ``"Streamflow"`` when blank.

    Args:
        river: Feature part from :func:`_name_parts`.

    Returns:
        The prefixed sentence-cased name, or `_cfg.event_name_fallback`
        when ``site_name`` is missing or blank.
    """
    named = F.when(F.trim(river) != "", F.concat(F.lit(_cfg.event_name_prefix), _sentence_case(river)))
    return F.coalesce(named, F.lit(_cfg.event_name_fallback))


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


def _danger_rating() -> Column:
    """The NWS flood category of the day's peak stage mapped onto the shared severity scale.

    Returns:
        ``none`` for ``no_flooding`` (the gauge is rated and nothing is
        expected), then ``low`` for ``action``, ``medium`` for ``minor``,
        ``high`` for ``moderate`` and ``extreme`` for ``major`` from
        `_cfg.danger_rating_map`; ``no_rating`` when the row carries no
        category the map knows — an empty category, ``NULL`` or an unexpected
        code (the NWS could not classify the day) — never ``NULL`` itself.
    """
    known = F.create_map(*[F.lit(item) for pair in _cfg.danger_rating_map.items() for item in pair])
    return F.coalesce(known[F.lower(F.trim(F.col("flood_category")))], F.lit(DangerRating.NO_RATING))


def _value_and_unit(column: str, name: str) -> tuple[Column, Column]:
    """``(value, unit)`` struct fields — the unit lives in its own ``<name>_unit`` key.

    Args:
        column: Source column holding the measurement.
        name: JSON key of the value; its unit is keyed ``<name>_unit``.

    Returns:
        The measurement aliased as *name* and the configured stage unit
        (``_cfg.stage_unit``) aliased as ``<name>_unit``; the unit is
        ``NULL`` — and so dropped from the JSON — exactly when the
        measurement itself is missing.
    """
    value = F.col(column)
    return value.alias(name), F.when(value.isNotNull(), F.lit(_cfg.stage_unit)).alias(f"{name}_unit")


def _other_metrics() -> Column:
    """The day's peak gauge height against the gauge's NWS thresholds as JSON.

    Returns:
        e.g. ``{"gauge_height": 7.8, "gauge_height_unit": "ft",
        "nws_gauge_id": "BRKM2", "action_stage": 5.0,
        "action_stage_unit": "ft", "minor_stage": 10.0,
        "minor_stage_unit": "ft", "moderate_stage": 12.0,
        "moderate_stage_unit": "ft", "major_stage": 14.0,
        "major_stage_unit": "ft"}``.

        ``gauge_height`` is the row's own peak stage (the highest
        instantaneous reading of that day); the ``*_stage`` thresholds
        and ``nws_gauge_id`` come from the flood-category reference
        (omitted entirely for a gauge it does not cover, and each
        threshold on its own when the NWS defines only some of them).
        Every measurement carries the configured stage unit (``ft``,
        ``_cfg.stage_unit``) in its own
        ``<name>_unit`` companion key rather than in the key name, and
        that unit key never outlives its value. A row with nothing to
        say — no stage and no reference — gets ``NULL`` rather than ``{}``.
    """
    payload = F.struct(
        *_value_and_unit("gauge_height", "gauge_height"),
        F.col("nws_gauge_id"),
        *(field for level in _cfg.flood_levels for field in _value_and_unit(f"{level}_stage_ft", f"{level}_stage")),
    )
    present = F.col("gauge_height").isNotNull()
    for level in _cfg.flood_levels:
        present = present | F.col(f"{level}_stage_ft").isNotNull()
    return F.when(present, F.to_json(payload)).otherwise(F.lit(None).cast("string"))


class UsgsNwisEtlScript(SingleEtlScript):
    """USGS NWIS streamflow → ``geo.usgs_nwis_etl`` (canonical event structure)."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_TABLE = _cfg.target_table
    DEDUP_KEYS = ("subsource_name", "event_start")

    def __init__(self, flood_categories_path: str | None = None) -> None:
        """Load the script with an explicit flood-category reference path.

        Args:
            flood_categories_path: CSV kept present by GEO_Download's
                ``USGS_NWIS`` downloader. ``None`` resolves
                `_cfg.flood_categories_file` below the data directory of
                ``config.json`` at first use.
        """
        self._flood_categories_path = flood_categories_path

    @property
    def flood_categories_path(self) -> str:
        """Absolute path of the flood-category reference CSV."""
        if self._flood_categories_path is None:
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            data_dir = os.path.normpath(os.path.join(project_root, AppConfig.from_json().data.directory))
            self._flood_categories_path = os.path.join(data_dir, _cfg.flood_categories_file)
        return self._flood_categories_path

    def _flood_categories(self, spark: SparkSession) -> DataFrame:
        """Read the NWS flood-category reference of the gauges.

        Args:
            spark: Active Spark session.

        Returns:
            One row per gauge carrying only the join key, the NWS gauge
            identifier and the four stage thresholds; an empty frame when
            the file is missing, so an absent reference leaves the rows
            untouched instead of failing the run.
        """
        if os.path.exists(self.flood_categories_path):
            frame = spark.read.csv(self.flood_categories_path, header=True, schema=_FLOOD_CATEGORY_SCHEMA)
        else:
            frame = spark.createDataFrame([], _FLOOD_CATEGORY_SCHEMA)
        return frame.select(
            "site_id",
            F.col("identifier").alias("nws_gauge_id"),
            *(F.col(f"{level}_stage_ft") for level in _cfg.flood_levels),
        )

    def _convert(self, df: DataFrame) -> DataFrame:
        """Map ``geo.usgs_nwis`` columns onto the canonical ETL structure.

        One row per gauge per measurement day per snapshot: the gauge's
        identity (site, name, point) describes the event while ``date``
        places the daily mean in time. The country is always ``US``; the
        subdivision is the stamped ``geocode_state`` of the gauge point.
        The flood-category reference is joined on ``site_id`` (broadcast —
        it is one row per gauge) and feeds ``danger_rating`` /
        ``other_metrics``.

        Args:
            df: Rows read from :attr:`SOURCE_TABLE`.

        Returns:
            The DataFrame in :attr:`TARGET_TABLE` column order.
        """
        river, place = _name_parts()
        categories = self._flood_categories(df.sparkSession)
        merged = df.join(F.broadcast(categories), ["site_id"], "left")

        return merged.select(
            F.lit(_cfg.source_name).alias("source_name"),
            F.col("site_id").alias("subsource_name"),
            _event_name(river).alias("event_name"),
            F.lit(_cfg.event_category).alias("event_category"),
            F.lit(None).cast("string").alias("event_subcategory"),
            _event_description().alias("event_description"),
            F.col("latitude"),
            F.col("longitude"),
            F.lit(_cfg.country_code).alias("country_code"),
            _state_province(F.col("geocode_state")).alias("state_province"),
            _place_location(river, place).alias("place_location"),
            F.lit(_cfg.metric_name).alias("metric_name"),
            F.coalesce(F.col("value"), F.lit(0.0)).alias("metric_value"),
            F.col("unit").alias("metric_unit"),
            _danger_rating().alias("danger_rating"),
            _other_metrics().alias("other_metrics"),
            F.col("date").cast("timestamp").alias("event_start"),
            F.lit(None).cast("timestamp").alias("event_end"),
            F.lit(None).cast("double").alias("last_latitude"),
            F.lit(None).cast("double").alias("last_longitude"),
            F.col(self.FILE_DATE_COL),
        )
