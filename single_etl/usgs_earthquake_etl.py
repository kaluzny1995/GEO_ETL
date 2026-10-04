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

import logging

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import LoadType
from utils.etl_utils import run_etl

SOURCE_TABLE = "usgs_earthquake"
TARGET_TABLE = "usgs_earthquake_etl"
FILE_DATE_COL = "file_date"
SOURCE_NAME = "USGS"
EVENT_CATEGORY = "earthquake"
METRIC_NAME = "magnitude"
UNKNOWN_COUNTRY_CODE = "ZZ"
UNIT_KM = "km"
UNIT_STATIONS = "stations"
EVENT_NAME_PREFIX = "Earthquake - "
EVENT_NAME_FALLBACK = "Earthquake"
US_STATE_NAMES = "United States"
US_STATE_CODES = (
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
    "PR",
    "VI",
    "GU",
    "AS",
    "MP",
)
US_STATE_FULL_NAMES = (
    "Alabama",
    "Alaska",
    "Arizona",
    "Arkansas",
    "California",
    "Colorado",
    "Connecticut",
    "Delaware",
    "Florida",
    "Georgia",
    "Hawaii",
    "Idaho",
    "Illinois",
    "Indiana",
    "Iowa",
    "Kansas",
    "Kentucky",
    "Louisiana",
    "Maine",
    "Maryland",
    "Massachusetts",
    "Michigan",
    "Minnesota",
    "Mississippi",
    "Missouri",
    "Montana",
    "Nebraska",
    "Nevada",
    "New Hampshire",
    "New Jersey",
    "New Mexico",
    "New York",
    "North Carolina",
    "North Dakota",
    "Ohio",
    "Oklahoma",
    "Oregon",
    "Pennsylvania",
    "Rhode Island",
    "South Carolina",
    "South Dakota",
    "Tennessee",
    "Texas",
    "Utah",
    "Vermont",
    "Virginia",
    "Washington",
    "West Virginia",
    "Wisconsin",
    "Wyoming",
    "Puerto Rico",
)
US_SUFFIXES = sorted((*US_STATE_CODES, *US_STATE_FULL_NAMES))
COUNTRY_CODE_MAP = {
    "Indonesia": "ID",
    "Colombia": "CO",
    "Chile": "CL",
    "Japan": "JP",
    "New Caledonia": "NC",
    "Canada": "CA",
    "U.S. Virgin Islands": "US",
    "Vanuatu": "VU",
    "Dominican Republic": "DO",
    "Papua New Guinea": "PG",
    "Russia": "RU",
    "Tonga": "TO",
    "Mexico": "MX",
    "MX": "MX",
    "Philippines": "PH",
    "China": "CN",
    "Peru": "PE",
    "New Zealand": "NZ",
    "Afghanistan": "AF",
    "Japan region": "JP",
    "Fiji": "FJ",
    "Argentina": "AR",
    "Timor Leste": "TL",
    "Solomon Islands": "SB",
    "Taiwan": "TW",
    "Tajikistan": "TJ",
    "Northern Mariana Islands": "US",
    "Greece": "GR",
    "Guatemala": "GT",
    "Iran": "IR",
    "Guam": "US",
    "Turkey": "TR",
    "India": "IN",
    "Somalia": "SO",
    "Wallis and Futuna": "WF",
    "Panama": "PA",
    "El Salvador": "SV",
    "Svalbard and Jan Mayen": "SJ",
    "Venezuela": "VE",
    "Ecuador": "EC",
    "Bolivia": "BO",
    "Yemen": "YE",
    "Azerbaijan": "AZ",
    "Saint Eustatius and Saba": "SX",
    "Saint Helena": "SH",
    "Romania": "RO",
    "Nepal": "NP",
    "Albania": "AL",
    "Lebanon": "LB",
    "Costa Rica": "CR",
    "Haiti": "HT",
    "Ethiopia": "ET",
    "Italy": "IT",
    "Bhutan": "BT",
    "Anguilla": "AI",
    "Martinique": "MQ",
    "Spain": "ES",
    "Austria": "AT",
    "Libya": "LY",
    "Kazakhstan": "KZ",
}
COUNTRY_NAME_BY_CODE = {"US": US_STATE_NAMES}
for _country, _iso_code in COUNTRY_CODE_MAP.items():
    COUNTRY_NAME_BY_CODE.setdefault(_iso_code, _country)


def _country_map() -> Column:
    """Literal ``country name → ISO code`` map column built from :data:`COUNTRY_CODE_MAP`."""
    return F.create_map(*[F.lit(item) for pair in COUNTRY_CODE_MAP.items() for item in pair])


def _country_name_map() -> Column:
    """Literal ``ISO code → country name`` map column built from :data:`COUNTRY_NAME_BY_CODE`."""
    return F.create_map(*[F.lit(item) for pair in COUNTRY_NAME_BY_CODE.items() for item in pair])


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
        ``US`` for a US state suffix, the :data:`COUNTRY_CODE_MAP` answer
        for an international suffix, the stamped country when the
        suffix cannot answer, ``UNKNOWN_COUNTRY_CODE`` when nothing
        resolves.
    """
    return F.coalesce(
        F.when(suffix.isin(US_SUFFIXES), F.lit("US")),
        _country_map()[suffix],
        F.when(geocode_country != "", geocode_country),
        F.lit(UNKNOWN_COUNTRY_CODE),
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
        F.when(suffix.isin(US_SUFFIXES), F.lit(US_STATE_NAMES)),
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
        The prefixed country name, or :data:`EVENT_NAME_FALLBACK`.
    """
    named = F.when(country.isNotNull() & (country != ""), F.concat(F.lit(EVENT_NAME_PREFIX), country))
    return F.coalesce(named, F.lit(EVENT_NAME_FALLBACK))


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
            F.when(depth.isNotNull(), F.lit(UNIT_KM)).alias("depth_unit"),
            nst.alias("nst"),
            F.when(nst.isNotNull(), F.lit(UNIT_STATIONS)).alias("nst_unit"),
        )
    )


def _convert(df: DataFrame) -> DataFrame:
    """Map ``geo.usgs_earthquake`` columns onto the canonical ETL structure.

    One row per seismic event: the epicentre and origin time place the
    rupture, the magnitude (with its ``magType``) measures it, and the
    ``place`` suffix supplies the country unless the stamped geocode
    columns have to.

    Args:
        df: Rows read from :data:`SOURCE_TABLE`.

    Returns:
        The DataFrame in :data:`TARGET_TABLE` column order.
    """
    suffix = _place_suffix()

    return df.select(
        F.lit(SOURCE_NAME).alias("source_name"),
        F.col("net").alias("subsource_name"),
        _event_name(_country_name(suffix, F.col("geocode_country"))).alias("event_name"),
        F.lit(EVENT_CATEGORY).alias("event_category"),
        F.col("event_type").alias("event_subcategory"),
        _event_description().alias("event_description"),
        F.col("latitude"),
        F.col("longitude"),
        _country_code(suffix, F.col("geocode_country")).alias("country_code"),
        _state_province(F.col("geocode_state")).alias("state_province"),
        F.col("place").alias("place_location"),
        F.lit(METRIC_NAME).alias("metric_name"),
        F.coalesce(F.col("mag"), F.lit(0.0)).alias("metric_value"),
        F.col("mag_type").alias("metric_unit"),
        F.lit(None).cast("string").alias("danger_rating"),
        _other_metrics().alias("other_metrics"),
        F.col("time").alias("event_start"),
        F.lit(None).cast("timestamp").alias("event_end"),
        F.lit(None).cast("double").alias("last_latitude"),
        F.lit(None).cast("double").alias("last_longitude"),
        F.col(FILE_DATE_COL),
    )


def run(
    spark: SparkSession,
    config: AppConfig,
    db_config: SupabaseConnectionConfig,
    *,
    load_type: LoadType | None = None,
    logger: logging.Logger | None = None,
) -> str:
    """Read ``geo.usgs_earthquake`` and append its converted rows to ``geo.usgs_earthquake_etl``.

    Delegates to :func:`utils.etl_utils.run_etl` with this module's
    :func:`_convert`; see there for the load-type contract.

    Args:
        spark: Active Spark session.
        config: Application configuration (DB schema, JDBC driver, logging).
        db_config: Supabase connection credentials.
        load_type: Load type forced by the caller, or ``None`` to detect it
            from the target table.
        logger: Optional logger; defaults to the runner's logger.

    Returns:
        A human-readable summary, e.g. ``"HISTORICAL: wrote to geo.usgs_earthquake_etl"``
        or ``"INCREMENTAL: nothing to do — usgs_earthquake_etl is up to date"``.
    """
    return run_etl(
        spark,
        config,
        db_config,
        SOURCE_TABLE,
        TARGET_TABLE,
        _convert,
        load_type=load_type,
        logger=logger,
    )
