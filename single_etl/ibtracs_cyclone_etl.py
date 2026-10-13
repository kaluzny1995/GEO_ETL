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
loads stay correct). The source states no country at all, so the country
and subdivision come from the ``geocode_country``/``geocode_state`` columns
stamped into the CSVs at download time (GEO_Download reverse-geocodes the
points within 100 km of land through ``data/reverse_geocode.csv`` and stamps
cached answers for the rest too), and a point the stamp found no country
for resolves to ``ZZ``. No reverse-geocoding API is called here.

Run::

    python run_etl.py SINGLE_ETL ibtracs_cyclone_etl
    python run_etl.py SINGLE_ETL ibtracs_cyclone_etl --load-type historical
    python run_etl.py SINGLE_ETL ibtracs_cyclone_etl --load-type incremental

**Load types** — ``HISTORICAL``, ``INCREMENTAL`` and the ``--load-type auto``
detection are described in :func:`utils.etl_utils.run_etl`.
"""

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F

from model.config.datasets import IbtracsCycloneEtlConfig, load_dataset
from model.enums import DangerRating
from single_etl.single_etl_script import SingleEtlScript

_cfg = load_dataset(__file__, "single_etl", IbtracsCycloneEtlConfig)


def _event_subcategory() -> Column:
    """The ATCF system status of the point, with the cross-agency ``nature`` as fallback.

    Returns:
        The ``usa_status`` word (`_cfg.status_words`), else the ``nature``
        word (`_cfg.nature_words`); ``NULL`` when neither reports a type
        (blank status next to ``nature = NR`` in non-US basins).
    """
    status = F.create_map(*[F.lit(item) for pair in _cfg.status_words.items() for item in pair])
    nature = F.create_map(*[F.lit(item) for pair in _cfg.nature_words.items() for item in pair])
    return F.coalesce(status[F.upper(F.trim(F.col("usa_status")))], nature[F.upper(F.trim(F.col("nature")))])


def _country_code(geocode_country: Column) -> Column:
    """The stamped country of the point, ``ZZ`` when the stamp found none.

    Args:
        geocode_country: ``geocode_country`` column stamped into the CSVs at
            download time.

    Returns:
        The point's ISO 3166-1 alpha-2 country code (the source carries no
        country) or :attr:`SingleEtlScript.UNKNOWN_CODE` for open water.
    """
    return F.when(geocode_country != "", geocode_country).otherwise(F.lit(SingleEtlScript.UNKNOWN_CODE))


def _state_province(geocode_state: Column) -> Column:
    """The stamped subdivision, ``NULL`` when the stamp found none.

    Args:
        geocode_state: ``geocode_state`` column stamped into the CSVs at
            download time.

    Returns:
        The point's subdivision code (``US-TX`` → ``TX``), or ``NULL``.
    """
    return F.when(geocode_state != "", geocode_state).otherwise(F.lit(None).cast("string"))


def _place_location() -> Column:
    """The IBTrACS region name: the full basin name extended with the sub-basin.

    Returns:
        ``North Atlantic - Gulf of Mexico`` / ``Eastern North Pacific -
        Central Pacific`` / … for a named sub-basin (`_cfg.subbasin_names`);
        just the basin's full name (`_cfg.basin_names`) for the unnamed
        sub-basins (``NA``, ``MM``); ``NULL`` when the basin itself is
        unknown.
    """
    basins = F.create_map(*[F.lit(item) for pair in _cfg.basin_names.items() for item in pair])
    subbasins = F.create_map(*[F.lit(item) for pair in _cfg.subbasin_names.items() for item in pair])
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


def _sshs_label() -> Column:
    """The Saffir-Simpson label of ``usa_sshs`` (`_cfg.sshs_labels`), ``NULL`` when it has no class.

    Returns:
        ``category 5`` … ``category 1``, ``tropical storm``,
        ``tropical depression``, ``disturbance`` and the remaining classes of
        `_cfg.sshs_labels`; ``NULL`` when the source reports no class (the
        native wording the source itself uses — it travels in ``other_metrics``,
        while :func:`_danger_rating` holds the shared scale).
    """
    labels = F.create_map(*[F.lit(item) for pair in _cfg.sshs_labels.items() for item in pair])
    return labels[F.col("usa_sshs").cast("string")]


def _danger_rating() -> Column:
    """The Saffir-Simpson class of ``usa_sshs`` mapped onto the shared severity scale.

    Returns:
        ``extreme`` for categories 4 and 5, ``high`` for category 3, ``medium``
        for categories 1 and 2, ``low`` for a tropical storm and every weaker
        system, and ``no_rating`` when the source reports no usable class —
        ``usa_sshs`` ``NULL``, the ``unknown`` class (``-5``) or a code
        `_cfg.danger_rating_map` does not list.
    """
    known = F.create_map(*[F.lit(item) for pair in _cfg.danger_rating_map.items() for item in pair])
    return F.coalesce(known[F.col("usa_sshs").cast("string")], F.lit(DangerRating.NO_RATING))


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
    """The point's Saffir-Simpson class and remaining measurements as a JSON object string.

    Returns:
        ``{"saffir_simpson": "category 1", "pressure": …, "pressure_unit": …,
        "speed": …, "speed_unit": …, "direction": …, "direction_unit": …,
        "gust": …, "gust_unit": …, "dist2land": …, "dist2land_unit": …,
        "landfall": …, "landfall_unit": …}`` — ``saffir_simpson`` keeps the
        source's own class wording (the shared severity scale in
        ``danger_rating`` merges categories 1+2 and 4+5), and every measurement
        is paired with its ``<name>_unit`` companion key (``mb``, ``kts``,
        ``degrees``, ``km``); measurements the point does not report are omitted
        together with their unit, ``NULL`` when it reports none of them.
    """
    metrics = F.to_json(
        F.struct(
            _sshs_label().alias("saffir_simpson"),
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


class IbtracsCycloneEtlScript(SingleEtlScript):
    """IBTrACS → ``geo.ibtracs_cyclone_etl`` (canonical event structure)."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_TABLE = _cfg.target_table
    DEDUP_KEYS = ("subsource_name", "event_name", "latitude", "longitude", "event_start")

    def _convert(self, df: DataFrame) -> DataFrame:
        """Map ``geo.ibtracs_cyclone`` columns onto the canonical ETL structure.

        One row per best-track position: the storm's identity (basin, name,
        intensity) repeats over its points while the coordinate, the wind and
        the dates describe the position itself. Country and subdivision are the
        stamped geocode columns; points the stamp could not answer resolve to
        ``ZZ`` / ``NULL``.

        Args:
            df: Rows read from :attr:`SOURCE_TABLE`.

        Returns:
            The DataFrame in :attr:`TARGET_TABLE` column order.
        """
        basin = F.trim(F.col("basin"))
        name = F.trim(F.col("name"))
        latest = Window.partitionBy("sid").orderBy(F.col("iso_time").desc())

        return df.select(
            F.lit(_cfg.source_name).alias("source_name"),
            F.when(basin != "", basin).otherwise(F.lit(None).cast("string")).alias("subsource_name"),
            F.when(name != "", name).otherwise(F.lit(None).cast("string")).alias("event_name"),
            F.lit(_cfg.event_category).alias("event_category"),
            _event_subcategory().alias("event_subcategory"),
            F.lit(None).cast("string").alias("event_description"),
            F.col("lat").alias("latitude"),
            F.col("lon").alias("longitude"),
            _country_code(F.col("geocode_country")).alias("country_code"),
            _state_province(F.col("geocode_state")).alias("state_province"),
            _place_location().alias("place_location"),
            F.lit(_cfg.metric_name).alias("metric_name"),
            _metric_value().alias("metric_value"),
            F.lit(_cfg.metric_unit).alias("metric_unit"),
            _danger_rating().alias("danger_rating"),
            _other_metrics().alias("other_metrics"),
            F.col("iso_time").alias("event_start"),
            F.col("iso_time").alias("event_end"),
            F.first("lat").over(latest).alias("last_latitude"),
            F.first("lon").over(latest).alias("last_longitude"),
            F.col(self.FILE_DATE_COL),
        )
