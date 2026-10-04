"""USGS NWIS streamflow → ``geo.usgs_nwis_etl`` (canonical event structure).

Reads the typed plain table ``geo.usgs_nwis`` and writes every gauge
reading into ``geo.usgs_nwis_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl``
tables. The event name and place are parsed from ``site_name`` around its
first ``NEAR``/``AT`` marker on the driver-free path; the gauge point's
``state_province`` is the ``geocode_state`` column stamped into the CSVs at
download time (empty → ``NULL``). No reverse-geocoding API is called here.

Run::

    python run_etl.py SINGLE_ETL usgs_nwis_etl
    python run_etl.py SINGLE_ETL usgs_nwis_etl --load-type historical
    python run_etl.py SINGLE_ETL usgs_nwis_etl --load-type incremental

**Load types** — ``HISTORICAL``, ``INCREMENTAL`` and the ``--load-type auto``
detection are described in :func:`utils.etl_utils.run_etl`.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from model.config.datasets import UsgsNwisEtlConfig, load_dataset
from single_etl.single_etl_script import SingleEtlScript

_cfg = load_dataset(__file__, "single_etl", UsgsNwisEtlConfig)


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


class UsgsNwisEtlScript(SingleEtlScript):
    """USGS NWIS streamflow → ``geo.usgs_nwis_etl`` (canonical event structure)."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_TABLE = _cfg.target_table

    def _convert(self, df: DataFrame) -> DataFrame:
        """Map ``geo.usgs_nwis`` columns onto the canonical ETL structure.

        One row per gauge per measurement day per snapshot: the gauge's
        identity (site, name, point) describes the event while ``date``
        places the daily mean in time. The country is always ``US``; the
        subdivision is the stamped ``geocode_state`` of the gauge point.

        Args:
            df: Rows read from :attr:`SOURCE_TABLE`.

        Returns:
            The DataFrame in :attr:`TARGET_TABLE` column order.
        """
        river, place = _name_parts()

        return df.select(
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
            F.lit(None).cast("string").alias("danger_rating"),
            F.lit(None).cast("string").alias("other_metrics"),
            F.col("date").cast("timestamp").alias("event_start"),
            F.lit(None).cast("timestamp").alias("event_end"),
            F.lit(None).cast("double").alias("last_latitude"),
            F.lit(None).cast("double").alias("last_longitude"),
            F.col(self.FILE_DATE_COL),
        )
