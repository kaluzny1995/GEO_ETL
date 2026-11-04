"""GDACS → ``geo.gdacs_event_etl`` (canonical event structure).

Reads the typed plain table ``geo.gdacs_event`` and writes every event
snapshot into ``geo.gdacs_event_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl``
tables. The source states an ISO 3166-1 alpha-3 ``country_code``, which the
script translates to alpha-2 (`_cfg.country_code_map` — the codes GDACS uses
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

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from model.config.datasets import GdacsEventEtlConfig, load_dataset
from model.enums import DangerRating
from single_etl.single_etl_script import SingleEtlScript
from utils.column_helpers import literal_map, state_province
from utils.metric_units import standardized_metric

_cfg = load_dataset(__file__, "single_etl", GdacsEventEtlConfig)


def _event_category() -> Column:
    """``event_category`` from ``event_type`` (``_cfg.category_map``), lowercased otherwise."""
    known = literal_map(_cfg.category_map)
    trimmed = F.trim(F.col("event_type"))
    return F.coalesce(known[trimmed], F.lower(trimmed))


def _event_subcategory(category: Column) -> Column:
    """The cyclone's intensity class, the category defaults, the drought kind from the text.

    Args:
        category: The row's ``event_category`` expression.

    Returns:
        ``tropical depression`` / ``tropical storm`` / ``hurricane`` for
        cyclones (GDACS's own class in ``severity_text``, ``Hurricane/Typhoon``
        counted as ``hurricane`` to match the EONET keyword, the category word
        ``storm`` itself when GDACS states no class), ``wildfire`` for
        fires, ``agricultural drought`` for droughts, and the shared defaults
        for the categories without a source subtype: ``other`` for
        earthquakes, ``water discharge`` for floods and ``volcano`` for
        volcanic events. Droughts GDACS does not qualify (and event types
        the category map does not know) stay ``NULL``.
    """
    text = F.lower(F.trim(F.col("severity_text")))
    drought_kind = F.regexp_extract(text, r"for ([a-z]+) drought", 1)
    intensity = (
        F.when(text.contains("tropical depression"), F.lit("tropical depression"))
        .when(text.contains("tropical storm"), F.lit("tropical storm"))
        .when(text.contains("hurricane"), F.lit("hurricane"))
    )
    return (
        F.when(category == "storm", F.coalesce(intensity, F.lit("storm")))
        .when(category == "earthquake", F.lit("other"))
        .when(category == "flood", F.lit("water discharge"))
        .when(category == "volcano", F.lit("volcano"))
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
        The row's ISO 3166-1 alpha-2 country code; a code `_cfg.country_code_map`
        does not know (or a missing one) resolves from the stamped column.
    """
    alpha2 = literal_map(_cfg.country_code_map)
    source_alpha2 = alpha2[F.coalesce(F.upper(F.trim(F.col("country_code"))), F.lit(""))]
    return (
        F.when(source_alpha2.isNotNull(), source_alpha2)
        .when(geocode_country != "", geocode_country)
        .otherwise(F.lit(SingleEtlScript.UNKNOWN_CODE))
    )


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
    """``severity``, with ``0`` ("not defined" for floods and eruptions) kept as ``0``.

    Returns:
        The hazard severity as a number; a ``0`` GDACS publishes for floods
        and eruptions ("not defined") stays ``0`` rather than becoming
        ``NULL``, so every row carries a value.
    """
    return F.coalesce(F.col("severity").cast("double"), F.lit(0.0))


def _metric_unit() -> Column:
    """``severity_unit``, ``NULL`` when blank."""
    trimmed = F.trim(F.col("severity_unit"))
    return F.when(trimmed != "", trimmed).otherwise(F.lit(None).cast("string"))


def _place_location() -> Column:
    """``country`` (region at the centroid or affected-country list), ``NULL`` when blank."""
    trimmed = F.trim(F.col("country"))
    return F.when(trimmed != "", trimmed).otherwise(F.lit(None).cast("string"))


def _danger_rating() -> Column:
    """The alert level mapped onto the shared severity scale (`_cfg.danger_rating_map`).

    Returns:
        ``no_rating`` for ``Green`` (no significant impact expected), ``medium``
        for ``Orange`` and ``high`` for ``Red``; ``no_rating`` also when the row
        carries no alert level the map knows (blank, ``NULL`` or an unexpected
        code) — the value never comes back ``NULL``, so ``no_rating`` marks both
        the green-alert rows and the rows GDACS does not rate.
    """
    known = literal_map(_cfg.danger_rating_map)
    return F.coalesce(known[F.lower(F.trim(F.col("alert_level")))], F.lit(DangerRating.NO_RATING))


class GdacsEventEtlScript(SingleEtlScript):
    """GDACS → ``geo.gdacs_event_etl`` (canonical event structure)."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_TABLE = _cfg.target_table
    DEDUP_KEYS = tuple(_cfg.dedup_keys)

    def _convert(self, df: DataFrame) -> DataFrame:
        """Map ``geo.gdacs_event`` columns onto the canonical ETL structure.

        One row per event snapshot: the alpha-3 ``country_code`` of the source
        wins, blank and unknown codes resolve from the stamped
        ``geocode_country``, and the subdivision is the stamped ``geocode_state``.
        Floods and eruptions define no severity (``severity = 0``); that
        ``0`` stays in ``metric_value`` rather than becoming ``NULL``, and
        their empty ``severity_unit`` leaves ``metric_unit`` ``NULL``.

        Args:
            df: Rows read from :attr:`SOURCE_TABLE`.

        Returns:
            The DataFrame in :attr:`TARGET_TABLE` column order.
        """
        category = _event_category()
        metric_value, metric_unit = standardized_metric(_metric_name(category), _metric_value(), _metric_unit())

        return df.select(
            F.lit(_cfg.source_name).alias("source_name"),
            F.col("source").alias("subsource_name"),
            F.col("event_name"),
            category.alias("event_category"),
            _event_subcategory(category).alias("event_subcategory"),
            F.lit(None).cast("string").alias("event_description"),
            F.col("latitude"),
            F.col("longitude"),
            _country_code(F.col("geocode_country")).alias("country_code"),
            state_province(F.col("geocode_state")).alias("state_province"),
            _place_location().alias("place_location"),
            _metric_name(category).alias("metric_name"),
            metric_value.alias("metric_value"),
            metric_unit.alias("metric_unit"),
            _danger_rating().alias("danger_rating"),
            F.lit(None).cast("string").alias("other_metrics"),
            F.col("from_date").alias("event_start"),
            F.col("to_date").alias("event_end"),
            F.lit(None).cast("double").alias("last_latitude"),
            F.lit(None).cast("double").alias("last_longitude"),
            F.col(self.FILE_DATE_COL),
        )
