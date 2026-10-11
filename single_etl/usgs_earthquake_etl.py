"""USGS earthquakes → ``geo.usgs_earthquake_etl`` (canonical event structure).

Reads the typed plain table ``geo.usgs_earthquake`` and writes every seismic
event into ``geo.usgs_earthquake_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl``
tables. The country is parsed from the ``place`` suffix on the driver-free
path (US state sets + the name→ISO map below); only places neither can
answer fall back to the ``geocode_country``/``geocode_state`` columns
stamped into the CSVs at download time (through GEO_Download's
``data/reverse_geocode.csv``). No reverse-geocoding API is called here.

Run::

    python run_etl.py SINGLE_ETL usgs_earthquake_etl
    python run_etl.py SINGLE_ETL usgs_earthquake_etl --load-type historical
    python run_etl.py SINGLE_ETL usgs_earthquake_etl --load-type incremental

**Load types** — ``HISTORICAL``, ``INCREMENTAL`` and the ``--load-type auto``
detection are described in :func:`utils.etl_utils.run_etl`.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from model.config.datasets import UsgsEarthquakeEtlConfig, load_dataset
from single_etl.single_etl_script import SingleEtlScript

_cfg = load_dataset(__file__, "single_etl", UsgsEarthquakeEtlConfig)
_us_suffixes = sorted((*_cfg.us_state_codes, *_cfg.us_state_full_names))
_country_name_by_code = {"US": _cfg.us_state_names}
for _country, _iso_code in _cfg.country_code_map.items():
    _country_name_by_code.setdefault(_iso_code, _country)


def _country_map() -> Column:
    """Literal ``country name → ISO code`` map column built from `_cfg.country_code_map`."""
    return F.create_map(*[F.lit(item) for pair in _cfg.country_code_map.items() for item in pair])


def _country_name_map() -> Column:
    """Literal ``ISO code → country name`` map column built from `_country_name_by_code`."""
    return F.create_map(*[F.lit(item) for pair in _country_name_by_code.items() for item in pair])


def _place_suffix() -> Column:
    """The token after the last ``,`` of ``place`` (trimmed), ``""`` when there is none.

    Returns:
        ``"CA"`` from ``9 km WSW of Idyllwild, CA``, ``"Chile"`` from an
        international place, ``""`` for a bare Flinn-Engdahl region.
    """
    return F.trim(F.regexp_extract(F.coalesce(F.col("place"), F.lit("")), r", ([^,]*)$", 1))


def _country_code(suffix: Column, geocode_country: Column) -> Column:
    """The ISO country code: place suffix parsed, else stamped geocode, else ``ZZ``.

    Args:
        suffix: Column from :func:`_place_suffix`.
        geocode_country: ``geocode_country`` column stamped into the CSVs at
            download time.

    Returns:
        ``US`` for a US state suffix, the `_cfg.country_code_map` answer
        for an international suffix, the stamped country when the
        suffix cannot answer, ``SingleEtlScript.UNKNOWN_CODE`` when nothing
        resolves.
    """
    return F.coalesce(
        F.when(suffix.isin(_us_suffixes), F.lit("US")),
        _country_map()[suffix],
        F.when(geocode_country != "", geocode_country),
        F.lit(SingleEtlScript.UNKNOWN_CODE),
    )


def _country_name(suffix: Column, geocode_country: Column) -> Column:
    """The country name for ``event_name``: place suffix, else stamped geocode, else ``NULL``.

    Args:
        suffix: Column from :func:`_place_suffix`.
        geocode_country: ``geocode_country`` column stamped into the CSVs at
            download time.

    Returns:
        ``United States`` for a US suffix, the suffix itself for a known
        international name, the stamped country's name (or its
        code when no name is known) for geocoded rows, ``NULL`` when the
        country stays unknown.
    """
    known_name = F.coalesce(
        F.when(suffix.isin(_us_suffixes), F.lit(_cfg.us_state_names)),
        F.when(_country_map()[suffix].isNotNull(), suffix),
    )
    geocoded_name = F.when(
        geocode_country != "",
        F.coalesce(_country_name_map()[F.trim(geocode_country)], geocode_country),
    )
    return F.coalesce(known_name, geocoded_name)


def _event_name(country: Column) -> Column:
    """``"Earthquake - <country>"``; ``"Earthquake"`` alone when no country resolves.

    Args:
        country: Column from :func:`_country_name`.

    Returns:
        The prefixed country name, or `_cfg.event_name_fallback`.
    """
    named = F.when(country.isNotNull() & (country != ""), F.concat(F.lit(_cfg.event_name_prefix), country))
    return F.coalesce(named, F.lit(_cfg.event_name_fallback))


def _state_province(geocode_state: Column) -> Column:
    """``net`` uppercased, the stamped subdivision when ``net`` is blank, else ``NULL``.

    Args:
        geocode_state: ``geocode_state`` column stamped into the CSVs at
            download time.

    Returns:
        ``ak`` → ``AK``; a blank ``net`` falls back to the stamped
        epicentre subdivision, and an empty answer is ``NULL``.
    """
    net = F.trim(F.col("net"))
    return (
        F.when(net != "", F.upper(net)).when(geocode_state != "", geocode_state).otherwise(F.lit(None).cast("string"))
    )


def _event_description() -> Column:
    """``"<event_type> <status>"`` (``earthquake reviewed``); a blank part is dropped."""
    event_type = F.trim(F.col("event_type"))
    status = F.trim(F.col("status"))
    return F.coalesce(
        F.when((event_type != "") & (status != ""), F.concat_ws(" ", event_type, status)),
        F.when(event_type != "", event_type),
        F.when(status != "", status),
        F.lit(None).cast("string"),
    )


def _other_metrics() -> Column:
    """Depth and station count with their unit keys as a JSON object string.

    Returns:
        ``{"depth": …, "depth_unit": "km", "nst": …, "nst_unit": "stations"}``;
        the ``nst`` keys (and, defensively, the ``depth`` keys) are omitted
        when the network reports no value — ``ignoreNullFields`` drops them
        from the JSON.
    """
    depth = F.col("depth")
    nst = F.col("nst")
    return F.to_json(
        F.struct(
            depth.alias("depth"),
            F.when(depth.isNotNull(), F.lit(_cfg.unit_km)).alias("depth_unit"),
            nst.alias("nst"),
            F.when(nst.isNotNull(), F.lit(_cfg.unit_stations)).alias("nst_unit"),
        )
    )


class UsgsEarthquakeEtlScript(SingleEtlScript):
    """USGS earthquakes → ``geo.usgs_earthquake_etl`` (canonical event structure)."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_TABLE = _cfg.target_table
    DEDUP_KEYS = ("event_start", "latitude", "longitude")

    def _convert(self, df: DataFrame) -> DataFrame:
        """Map ``geo.usgs_earthquake`` columns onto the canonical ETL structure.

        One row per seismic event: the epicentre and origin time place the
        rupture, the magnitude (with its ``magType``) measures it, and the
        ``place`` suffix supplies the country unless the stamped geocode
        columns have to.

        Args:
            df: Rows read from :attr:`SOURCE_TABLE`.

        Returns:
            The DataFrame in :attr:`TARGET_TABLE` column order.
        """
        suffix = _place_suffix()

        return df.select(
            F.lit(_cfg.source_name).alias("source_name"),
            F.col("net").alias("subsource_name"),
            _event_name(_country_name(suffix, F.col("geocode_country"))).alias("event_name"),
            F.lit(_cfg.event_category).alias("event_category"),
            F.col("event_type").alias("event_subcategory"),
            _event_description().alias("event_description"),
            F.col("latitude"),
            F.col("longitude"),
            _country_code(suffix, F.col("geocode_country")).alias("country_code"),
            _state_province(F.col("geocode_state")).alias("state_province"),
            F.col("place").alias("place_location"),
            F.lit(_cfg.metric_name).alias("metric_name"),
            F.coalesce(F.col("mag"), F.lit(0.0)).alias("metric_value"),
            F.col("mag_type").alias("metric_unit"),
            F.lit(None).cast("string").alias("danger_rating"),
            _other_metrics().alias("other_metrics"),
            F.col("time").alias("event_start"),
            F.lit(None).cast("timestamp").alias("event_end"),
            F.lit(None).cast("double").alias("last_latitude"),
            F.lit(None).cast("double").alias("last_longitude"),
            F.col(self.FILE_DATE_COL),
        )
