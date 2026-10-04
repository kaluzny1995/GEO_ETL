"""Avalanche.org → ``geo.avalanche_org_etl`` (canonical event structure).

Reads the typed plain table ``geo.avalanche_org`` and writes every forecast
zone/day into ``geo.avalanche_org_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl`` tables.
Zone coordinates come from the source's own ``latitude``/``longitude`` columns —
polygon centroids computed by GEO_Download when the daily CSVs are written, so
this script performs no lookup and no network access.

Run::

    python run_etl.py SINGLE_ETL avalanche_org_etl
    python run_etl.py SINGLE_ETL avalanche_org_etl --load-type historical
    python run_etl.py SINGLE_ETL avalanche_org_etl --load-type incremental

**Load types** — ``HISTORICAL``, ``INCREMENTAL`` and the ``--load-type auto``
detection are described in :func:`utils.etl_utils.run_etl`.

**Adding the next source** — copy this file to ``single_etl/<name>_etl.py``,
point ``SOURCE_TABLE``/``TARGET_TABLE`` at its plain and ETL tables, rewrite
:func:`_convert` for its columns, then register it in ``jobs/__init__.py``::

    from single_etl.<name>_etl import <Name>Script
    SingleEtlFactory.register("<name>_etl", <Name>Script())
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from single_etl.single_etl_script import SingleEtlScript

SOURCE_NAME = "AVALANCHE_ORG"
COUNTRY_CODE = "US"
EVENT_CATEGORY = "avalanche"
METRIC_NAME = "magnitude"


def _danger_rating() -> Column:
    """The zone's danger rating, or ``NULL`` while the zone is unrated.

    The source marks unrated zones with ``danger_level = -1`` and a rating text
    of ``no rating``; neither is a rating, so both map to ``NULL``.
    """
    rating = F.trim(F.col("danger_rating"))
    unrated = (F.col("danger_level") <= 0) | (F.lower(rating).isin("", "no rating", "no_rating"))
    return F.when(~unrated, rating)


class AvalancheOrgEtlScript(SingleEtlScript):
    """Avalanche.org → ``geo.avalanche_org_etl`` (canonical event structure)."""

    SOURCE_TABLE = "avalanche_org"
    TARGET_TABLE = "avalanche_org_etl"

    def _convert(self, df: DataFrame) -> DataFrame:
        """Map ``geo.avalanche_org`` columns onto the canonical ETL structure.

        Every zone carries its polygon centroid as ``latitude``/``longitude``
        (``NULL`` when the source row has no coordinates, i.e. the download found
        no usable geometry). A danger level is a rating, not a measurement, so
        ``metric_value`` and ``metric_unit`` stay ``NULL`` and unrated zones
        (``danger_level = -1``) additionally get a ``NULL`` ``danger_rating``.

        Each source file holds the analysis day plus the next day's forecast (the
        shared ``[D, D+1]`` download range), and ``date`` is not part of the
        canonical structure — while both rows share one forecast window (the
        off-season "no rating" product), they project to identical rows. The
        trailing :meth:`DataFrame.distinct` collapses such copies, so the target
        keeps one row per forecast statement per ``file_date``; rows that differ
        (e.g. the next day's own validity window) are kept as their own snapshot.

        Args:
            df: Rows read from :attr:`SOURCE_TABLE`.

        Returns:
            The DataFrame in :attr:`TARGET_TABLE` column order, without duplicate rows.
        """
        return df.select(
            F.lit(SOURCE_NAME).alias("source_name"),
            F.col("center_name").alias("subsource_name"),
            F.lit(None).cast("string").alias("event_name"),
            F.lit(EVENT_CATEGORY).alias("event_category"),
            F.lit(None).cast("string").alias("event_subcategory"),
            F.lit(None).cast("string").alias("event_description"),
            F.col("latitude"),
            F.col("longitude"),
            F.lit(COUNTRY_CODE).alias("country_code"),
            F.col("state").alias("state_province"),
            F.col("zone_name").alias("place_location"),
            F.lit(METRIC_NAME).alias("metric_name"),
            F.lit(None).cast("double").alias("metric_value"),
            F.lit(None).cast("string").alias("metric_unit"),
            _danger_rating().alias("danger_rating"),
            F.lit(None).cast("string").alias("other_metrics"),
            F.col("forecast_start").alias("event_start"),
            F.col("forecast_end").alias("event_end"),
            F.lit(None).cast("double").alias("last_latitude"),
            F.lit(None).cast("double").alias("last_longitude"),
            F.col(self.FILE_DATE_COL),
        ).distinct()
