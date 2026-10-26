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

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from model.config.datasets import NifcFireEtlConfig, load_dataset
from model.enums import DangerRating
from single_etl.single_etl_script import SingleEtlScript
from utils.metric_units import standardized_metric

_cfg = load_dataset(__file__, "single_etl", NifcFireEtlConfig)


def _event_subcategory() -> Column:
    """``event_subcategory`` from ``fire_type`` (`_cfg.fire_type_map`), the category word when unknown.

    Returns:
        ``wildfire`` / ``prescribed fire`` for the source's own ``WF`` /
        ``RX`` codes; a blank or unknown ``fire_type`` (a perimeter the layer
        types as nothing) resolves to ``fire``.
    """
    known = F.create_map(*[F.lit(item) for pair in _cfg.fire_type_map.items() for item in pair])
    return F.coalesce(known[F.trim(F.col("fire_type"))], F.lit("fire"))


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
    return F.to_json(
        F.struct(percent.alias("percent_contained"), F.lit(_cfg.percent_unit).alias("percent_contained_unit"))
    )


class NifcFireEtlScript(SingleEtlScript):
    """NIFC → ``geo.nifc_fire_etl`` (canonical event structure)."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_TABLE = _cfg.target_table
    DEDUP_KEYS = ("event_name", "latitude", "longitude", "event_start")

    def _convert(self, df: DataFrame) -> DataFrame:
        """Map ``geo.nifc_fire`` columns onto the canonical ETL structure.

        One row per perimeter snapshot: the fire's identity (name, type,
        jurisdiction, acres, containment) describes the perimeter while the
        centroid and ``created_on`` place it in space and time.
        The source's ``state`` wins; the pre-state vintage resolves from the
        stamped ``geocode_state``. ``unit_name`` ships with the row, so no
        unit lookup runs here.

        Args:
            df: Rows read from :attr:`SOURCE_TABLE`.

        Returns:
            The DataFrame in :attr:`TARGET_TABLE` column order.
        """
        place_location = _place_location()
        metric_value, metric_unit = standardized_metric(
            F.lit(_cfg.metric_name), F.coalesce(F.col("acres"), F.lit(0.0)), F.lit(_cfg.metric_unit)
        )

        return df.select(
            F.lit(_cfg.source_name).alias("source_name"),
            F.lit(_cfg.subsource_name).alias("subsource_name"),
            _event_name().alias("event_name"),
            F.lit(_cfg.event_category).alias("event_category"),
            _event_subcategory().alias("event_subcategory"),
            F.lit(None).cast("string").alias("event_description"),
            F.col("latitude"),
            F.col("longitude"),
            F.lit(_cfg.country_code).alias("country_code"),
            _state_province(F.col("geocode_state")).alias("state_province"),
            place_location.alias("place_location"),
            F.lit(_cfg.metric_name).alias("metric_name"),
            metric_value.alias("metric_value"),
            metric_unit.alias("metric_unit"),
            F.lit(DangerRating.NO_RATING).alias("danger_rating"),
            _other_metrics().alias("other_metrics"),
            F.col("created_on").alias("event_start"),
            F.lit(None).cast("timestamp").alias("event_end"),
            F.lit(None).cast("double").alias("last_latitude"),
            F.lit(None).cast("double").alias("last_longitude"),
            F.col(self.FILE_DATE_COL),
        )
