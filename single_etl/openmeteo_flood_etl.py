"""Open-Meteo flood → ``geo.openmeteo_flood_etl`` (canonical event structure).

Reads the typed plain table ``geo.openmeteo_flood`` and writes every river
discharge snapshot into ``geo.openmeteo_flood_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl``
tables. The ISO ``country_code`` comes from the script's own name map over
the source's ``country`` full names (unknown names fall back to the
``geocode_country`` column stamped into the CSVs at download time), and the
reference point's ``state_province`` is the stamped ``geocode_state``
column. No reverse-geocoding API is called here.

Next to the plain table the script joins the threshold reference CSV
``data/openmeteo_discharge_thresholds.csv`` (built by GEO_Download's
``run_openmeteo_thresholds.py``) on ``river_name`` + ``country``, so a row
can say more than "the discharge was X": ``other_metrics`` carries the
river's baseline percentiles, the configured flood threshold and the
ratio the day's discharge stands in it, and ``danger_rating`` labels the
reference levels the day's discharge exceeded (``Q95``, ``Q99``). A river
the reference does not cover (or a missing file) simply keeps both
``NULL``.

Run::

    python run_etl.py SINGLE_ETL openmeteo_flood_etl
    python run_etl.py SINGLE_ETL openmeteo_flood_etl --load-type historical
    python run_etl.py SINGLE_ETL openmeteo_flood_etl --load-type incremental

**Load types** — ``HISTORICAL``, ``INCREMENTAL`` and the ``--load-type auto``
detection are described in :func:`utils.etl_utils.run_etl`.
"""

import os

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from model.config import AppConfig
from model.config.datasets import OpenMeteoFloodEtlConfig, load_dataset
from single_etl.single_etl_script import SingleEtlScript

_cfg = load_dataset(__file__, "single_etl", OpenMeteoFloodEtlConfig)

PERCENTILE_LEVELS: tuple[int, ...] = (50, 90, 95, 99)
"""Reference levels the threshold CSV carries — ``discharge_p50_m3s`` … ``discharge_p99_m3s``."""

_ENSEMBLE_LEVELS: tuple[str, ...] = ("mean", "median", "p25", "p75")
"""GloFAS ensemble statistics the downloader stamps onto forecast days."""

_THRESHOLD_SCHEMA = StructType(
    [
        StructField("river_name", StringType()),
        StructField("country", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("baseline_days", IntegerType()),
        StructField("baseline_start", StringType()),
        StructField("baseline_end", StringType()),
        *(StructField(f"discharge_p{level}_m3s", DoubleType()) for level in PERCENTILE_LEVELS),
    ]
)
"""Columns of the threshold reference, in the order ``run_openmeteo_thresholds.py`` writes them."""


def _threshold_column(level: int) -> str:
    """Name of the reference-level column (``95`` → ``discharge_p95_m3s``).

    Args:
        level: Percentile level.

    Returns:
        The threshold CSV column holding that level.

    Raises:
        ValueError: When ``threshold_percentile`` in ``config.json`` names a
            level the reference does not carry.
    """
    if level not in PERCENTILE_LEVELS:
        raise ValueError(
            f"threshold_percentile={_cfg.threshold_percentile} has no column in the threshold reference "
            f"(available levels: {', '.join(str(level) for level in PERCENTILE_LEVELS)})"
        )
    return f"discharge_p{level}_m3s"


def _threshold_level() -> int:
    """The configured flood threshold as a reference level (``0.95`` → ``95``)."""
    return round(_cfg.threshold_percentile * 100)


def _country_code(geocode_country: Column) -> Column:
    """The ISO country code: source name mapped, else stamped geocode, else ``ZZ``.

    Args:
        geocode_country: ``geocode_country`` column stamped into the CSVs at
            download time.

    Returns:
        `_cfg.country_code_map` applied to the trimmed source ``country``;
        a name the map does not know falls back to the stamped country of
        the reference point, and an empty stamp counts as a miss
        (``SingleEtlScript.UNKNOWN_CODE``).
    """
    mapping = F.create_map(*[F.lit(item) for pair in _cfg.country_code_map.items() for item in pair])
    return F.coalesce(
        mapping[F.trim(F.col("country"))],
        F.when(geocode_country != "", geocode_country),
        F.lit(SingleEtlScript.UNKNOWN_CODE),
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


def _discharge_present() -> Column:
    """Whether the row has a threshold or an ensemble answer to report."""
    present = F.col(_threshold_column(_threshold_level())).isNotNull()
    for level in _ENSEMBLE_LEVELS:
        present = present | F.col(f"discharge_{level}_m3s").isNotNull()
    return present


def _other_metrics() -> Column:
    """The threshold reference and the ensemble spread of the day as JSON.

    Returns:
        e.g. ``{"discharge_p50_m3s": 133044.0, …, "discharge_p99_m3s":
        226985.0, "threshold_percentile": 0.95, "threshold_m3s": 215774.0,
        "threshold_unit": "m³/s", "exceedance_ratio": 0.26,
        "threshold_exceeded": false, "baseline_days": 10870,
        "baseline_start": "1997-01-01", "baseline_end": "2026-10-05",
        "ensemble_mean_m3s": 55094.71, …}``.

        The four ``discharge_p*_m3s`` percentiles and the ``baseline_*``
        provenance come from the reference (all omitted for a river it does
        not cover); ``threshold_*`` repeats the configured reference level as
        the one flood flag to test, and ``exceedance_ratio`` is the day's
        discharge divided by it (``threshold_exceeded`` is the same test as a
        flag: strictly above 1 — a flat baseline that sits exactly on the
        threshold is never exceeded). The ``ensemble_*`` keys are the GloFAS
        ensemble statistics of the day, present on forecast days only. Every
        key a row cannot answer is omitted together with the others that share
        its source, and a row with nothing to say gets ``NULL`` rather than
        ``{}``.
    """
    threshold_level = _threshold_level()
    threshold_column = _threshold_column(threshold_level)
    has_threshold = F.col(threshold_column).isNotNull()
    discharge = F.col("discharge_m3s")
    payload = F.struct(
        *(F.col(f"discharge_p{level}_m3s") for level in PERCENTILE_LEVELS),
        F.when(has_threshold, F.lit(_cfg.threshold_percentile)).alias("threshold_percentile"),
        F.when(has_threshold, F.col(threshold_column)).alias("threshold_m3s"),
        F.when(has_threshold, F.lit(_cfg.metric_unit)).alias("threshold_unit"),
        F.when(has_threshold, discharge / F.col(threshold_column)).alias("exceedance_ratio"),
        F.when(has_threshold, discharge > F.col(threshold_column)).alias("threshold_exceeded"),
        F.col("baseline_days"),
        F.col("baseline_start"),
        F.col("baseline_end"),
        *(F.col(f"discharge_{level}_m3s").alias(f"ensemble_{level}_m3s") for level in _ENSEMBLE_LEVELS),
    )
    return F.when(_discharge_present(), F.to_json(payload)).otherwise(F.lit(None).cast("string"))


def _danger_rating() -> Column:
    """The highest reference level the day's discharge exceeded, else ``NULL``.

    Returns:
        ``"Q99"`` for a discharge strictly above the river's ``Q99``,
        ``"Q95"`` for one strictly above ``Q95`` — the configured threshold
        sets the floor, so a day the flood flag does not fire for always
        rates ``NULL``. The comparison is strict: a river whose baseline is
        flat (every day sits exactly on its own Q95, e.g. an all-zero
        GloFAS series) is never flagged. Without a threshold reference
        (river unknown to the CSV, or the file missing) every day rates
        ``NULL``.
    """
    discharge = F.col("discharge_m3s")
    rating = F.lit(None).cast("string")
    ladder = [level for level in sorted(PERCENTILE_LEVELS) if level >= _threshold_level()]
    for level in ladder:
        rating = F.when(discharge > F.col(_threshold_column(level)), F.lit(f"Q{level}")).otherwise(rating)
    return rating


class OpenMeteoFloodEtlScript(SingleEtlScript):
    """Open-Meteo flood → ``geo.openmeteo_flood_etl`` (canonical event structure)."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_TABLE = _cfg.target_table

    def __init__(self, thresholds_path: str | None = None) -> None:
        """Load the script with an explicit threshold reference path.

        Args:
            thresholds_path: CSV built by ``run_openmeteo_thresholds.py``.
                ``None`` resolves ``_cfg.thresholds_file`` below the data
                directory of ``config.json`` at first use.
        """
        self._thresholds_path = thresholds_path

    @property
    def thresholds_path(self) -> str:
        """Absolute path of the threshold reference CSV."""
        if self._thresholds_path is None:
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            data_dir = os.path.normpath(os.path.join(project_root, AppConfig.from_json().data.directory))
            self._thresholds_path = os.path.join(data_dir, _cfg.thresholds_file)
        return self._thresholds_path

    def _thresholds(self, spark: SparkSession) -> DataFrame:
        """Read the threshold reference, without its reference coordinates.

        Args:
            spark: Active Spark session.

        Returns:
            One row per river of the reference; an empty frame when the file
            is missing, so an unbuilt reference leaves the rows untouched
            instead of failing the run. The reference coordinates are dropped
            — the source row already has its own.
        """
        if not os.path.exists(self.thresholds_path):
            frame = spark.createDataFrame([], _THRESHOLD_SCHEMA)
        else:
            frame = spark.read.csv(self.thresholds_path, header=True, schema=_THRESHOLD_SCHEMA)
        return frame.drop("latitude", "longitude")

    def _convert(self, df: DataFrame) -> DataFrame:
        """Map ``geo.openmeteo_flood`` columns onto the canonical ETL structure.

        One row per river discharge value snapshot: the river's identity (name,
        country, reference point) describes the event while ``date`` places the
        GloFAS 24-h mean in time. The source's country name wins; unknown names
        fall back to the stamped ``geocode_country``, which also supplies the
        subdivision through ``geocode_state``. The threshold reference is
        joined on ``river_name`` + ``country`` (broadcast — the reference is a
        dozen rows per river) and feeds ``danger_rating`` / ``other_metrics``.

        Args:
            df: Rows read from :attr:`SOURCE_TABLE`.

        Returns:
            The DataFrame in :attr:`TARGET_TABLE` column order.
        """
        thresholds = self._thresholds(df.sparkSession)
        merged = df.join(F.broadcast(thresholds), ["river_name", "country"], "left")
        return merged.select(
            F.lit(_cfg.source_name).alias("source_name"),
            F.lit(_cfg.subsource_name).alias("subsource_name"),
            _event_name().alias("event_name"),
            F.lit(_cfg.event_category).alias("event_category"),
            F.lit(None).cast("string").alias("event_subcategory"),
            F.lit(None).cast("string").alias("event_description"),
            F.col("latitude"),
            F.col("longitude"),
            _country_code(F.col("geocode_country")).alias("country_code"),
            _state_province(F.col("geocode_state")).alias("state_province"),
            _place_location().alias("place_location"),
            F.lit(_cfg.metric_name).alias("metric_name"),
            F.coalesce(F.col("discharge_m3s"), F.lit(0.0)).alias("metric_value"),
            F.lit(_cfg.metric_unit).alias("metric_unit"),
            _danger_rating().alias("danger_rating"),
            _other_metrics().alias("other_metrics"),
            F.col("date").cast("timestamp").alias("event_start"),
            F.lit(None).cast("timestamp").alias("event_end"),
            F.lit(None).cast("double").alias("last_latitude"),
            F.lit(None).cast("double").alias("last_longitude"),
            F.col(self.FILE_DATE_COL),
        )
