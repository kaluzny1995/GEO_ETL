"""USGS volcanic activity notices → ``geo.usgs_volcano_etl`` (canonical event structure).

Reads the typed plain table ``geo.usgs_volcano`` and writes every notice row
into ``geo.usgs_volcano_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl``
tables. The notice details (coordinates, region, status line and full notice
type) are already stamped into the source CSVs by the downloader in
GEO_Download and arrive through the plain table, and so is the reverse-geocoded
subdivision of the notice point (``geocode_state``, stamped at download time
through GEO_Download's ``data/reverse_geocode.csv``): this ETL only reads that
column — ``ZZ`` when it is empty (e.g. the Ahyi Seamount ocean point). No
reverse-geocoding API is called here.

Run::

    python run_etl.py SINGLE_ETL usgs_volcano_etl
    python run_etl.py SINGLE_ETL usgs_volcano_etl --load-type historical
    python run_etl.py SINGLE_ETL usgs_volcano_etl --load-type incremental

**Load types** — ``HISTORICAL``, ``INCREMENTAL`` and the ``--load-type auto``
detection are described in :func:`utils.etl_utils.run_etl`.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from single_etl.single_etl_script import SingleEtlScript

SOURCE_NAME = "USGS"
EVENT_CATEGORY = "volcano"
METRIC_NAME = "alert_level"
COUNTRY_CODE = "US"
EVENT_NAME_PREFIX = "Volcano - "
EVENT_NAME_FALLBACK = "Volcano"
ALERT_SCALE = {
    "NORMAL": 0.0,
    "ADVISORY": 1.0,
    "WATCH": 2.0,
    "WARNING": 3.0,
}


def _event_name() -> Column:
    """``"Volcano - <volcano_name>"``; ``"Volcano"`` when the name is blank."""
    name = F.trim(F.col("volcano_name"))
    named = F.when(name != "", F.concat(F.lit(EVENT_NAME_PREFIX), name))
    return F.coalesce(named, F.lit(EVENT_NAME_FALLBACK))


def _event_subcategory() -> Column:
    """The full notice type stamped by the downloader, falling back to the feed's own code.

    Returns:
        ``Daily Update``, ``Weekly Update``, ``Volcano Activity
        Notification`` … from ``notice_type_name``; the source
        ``notice_type`` column (``DU``, ``WU``, ``VV``) answers when the
        stamped name is missing, and a blank in both is ``NULL``.
    """
    name = F.trim(F.col("notice_type_name"))
    return F.coalesce(F.when(name != "", name), F.trim(F.col("notice_type")))


def _place_location(region: Column) -> Column:
    """``"<volcano_name>, <region>"``; without a region only the name remains.

    Args:
        region: ``region`` column stamped by the downloader (``Aleutians``).

    Returns:
        Both parts joined with a comma, the name alone when the region is
        missing, ``NULL`` when the volcano name is blank.
    """
    name = F.trim(F.col("volcano_name"))
    region = F.when(region != "", region)
    return F.when(
        name != "",
        F.when(region != "", F.concat_ws(", ", name, region)).otherwise(name),
    ).otherwise(F.lit(None).cast("string"))


def _metric_value() -> Column:
    """The USGS alert scale as a number; an unknown code stays ``NULL``.

    Returns:
        ``NORMAL`` → ``0.0``, ``ADVISORY`` → ``1.0``, ``WATCH`` → ``2.0``,
        ``WARNING`` → ``3.0`` (case-insensitive); ``NULL`` for a blank or
        unrecognised level — an unknown level must not read as ``NORMAL``.
    """
    scale = F.create_map(*[F.lit(item) for pair in ALERT_SCALE.items() for item in pair])
    return scale[F.upper(F.trim(F.coalesce(F.col("alert_level"), F.lit(""))))]


def _state_province(state: Column) -> Column:
    """The stamped subdivision of the notice point, ``ZZ`` when it found none.

    Args:
        state: ``geocode_state`` column stamped into the CSVs at download time.

    Returns:
        The stamped answer (``AK``, ``HI``, ``MP``); an empty answer (the
        Ahyi Seamount sits in open water) or a ``NULL`` (no coordinate)
        resolves to :attr:`SingleEtlScript.UNKNOWN_CODE`.
    """
    return F.coalesce(F.when(state != "", state), F.lit(SingleEtlScript.UNKNOWN_CODE))


def _description(synopsis: Column) -> Column:
    """The notice's one-line status (``synopsis``), ``NULL`` when blank.

    Args:
        synopsis: ``synopsis`` column stamped by the downloader.

    Returns:
        e.g. ``AVO Great Sitkin ORANGE/WATCH - Slow eruption of lava …``;
        a blank line resolves to ``NULL``.
    """
    trimmed = F.trim(synopsis)
    return F.when(trimmed != "", trimmed)


class UsgsVolcanoEtlScript(SingleEtlScript):
    """USGS volcanic activity notices → ``geo.usgs_volcano_etl`` (canonical event structure)."""

    SOURCE_TABLE = "usgs_volcano"
    TARGET_TABLE = "usgs_volcano_etl"

    def _convert(self, df: DataFrame) -> DataFrame:
        """Map ``geo.usgs_volcano`` columns onto the canonical ETL structure.

        One row per volcano per notice per snapshot: the feed supplies the
        volcano, observatory, alert level and notice time, the downloader-stamped
        detail columns supply the coordinates, region, status line and full
        notice type — the country is always ``US`` and the subdivision is the
        stamped ``geocode_state`` of the notice point.

        Args:
            df: Rows read from :attr:`SOURCE_TABLE`.

        Returns:
            The DataFrame in :attr:`TARGET_TABLE` column order.
        """
        return df.select(
            F.lit(SOURCE_NAME).alias("source_name"),
            F.col("observatory_name").alias("subsource_name"),
            _event_name().alias("event_name"),
            F.lit(EVENT_CATEGORY).alias("event_category"),
            _event_subcategory().alias("event_subcategory"),
            _description(F.col("synopsis")).alias("event_description"),
            F.col("latitude"),
            F.col("longitude"),
            F.lit(COUNTRY_CODE).alias("country_code"),
            _state_province(F.col("geocode_state")).alias("state_province"),
            _place_location(F.col("region")).alias("place_location"),
            F.lit(METRIC_NAME).alias("metric_name"),
            _metric_value().alias("metric_value"),
            F.lit(None).cast("string").alias("metric_unit"),
            F.col("alert_level").alias("danger_rating"),
            F.lit(None).cast("string").alias("other_metrics"),
            F.col("notice_date").alias("event_start"),
            F.lit(None).cast("timestamp").alias("event_end"),
            F.lit(None).cast("double").alias("last_latitude"),
            F.lit(None).cast("double").alias("last_longitude"),
            F.col(self.FILE_DATE_COL),
        )
