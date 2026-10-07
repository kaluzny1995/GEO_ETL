"""USGS volcanic activity notices → ``geo.usgs_volcano_etl`` (canonical event structure).

Reads the typed plain table ``geo.usgs_volcano`` and writes every notice row
into ``geo.usgs_volcano_etl`` using the schema from
``model/etl/event_etl.py``, so it can be merged with the other ``*_etl``
tables. The notice details (coordinates, region, status line and full notice
type) are already stamped into the source CSVs by the downloader in
GEO_Download and arrive through the plain table, and so is the reverse-geocoded
subdivision of the notice point (``geocode_state``, stamped at download time
through GEO_Download's ``data/reverse_geocode.csv``) and the volcano's
significant-eruption record from the NCEI/WDS Significant Volcanic Eruptions
Database (``vei``, ``casualties``, ``damage`` — the largest VEI, total deaths
and total damage in millions of USD over all of the volcano's significant
eruptions): this ETL only reads those columns. No reverse-geocoding API is
called here.

Run::

    python run_etl.py SINGLE_ETL usgs_volcano_etl
    python run_etl.py SINGLE_ETL usgs_volcano_etl --load-type historical
    python run_etl.py SINGLE_ETL usgs_volcano_etl --load-type incremental

**Load types** — ``HISTORICAL``, ``INCREMENTAL`` and the ``--load-type auto``
detection are described in :func:`utils.etl_utils.run_etl`.
"""

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from model.config.datasets import UsgsVolcanoEtlConfig, load_dataset
from single_etl.single_etl_script import SingleEtlScript

_cfg = load_dataset(__file__, "single_etl", UsgsVolcanoEtlConfig)


def _event_name() -> Column:
    """``"Volcano - <volcano_name>"``; ``"Volcano"`` when the name is blank."""
    name = F.trim(F.col("volcano_name"))
    named = F.when(name != "", F.concat(F.lit(_cfg.event_name_prefix), name))
    return F.coalesce(named, F.lit(_cfg.event_name_fallback))


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
    """The volcano's largest significant-eruption VEI, ``NULL`` when NCEI has no record.

    Returns:
        e.g. ``4.0`` for Kīlauea (the maximum VEI over all of its
        significant eruptions in the NCEI/WDS database); ``NULL`` for a
        volcano NCEI does not list (e.g. Great Sitkin).
    """
    return F.col("vei").cast("double")


def _danger_rating() -> Column:
    """The VEI as a readable class (``VEI 4``), ``NULL`` when NCEI has no record.

    Returns:
        e.g. ``VEI 4`` for Kīlauea; ``NULL`` for a volcano NCEI does not
        list — an unknown VEI must not read as ``VEI 0``.
    """
    vei = F.col("vei")
    return F.when(vei.isNotNull(), F.concat_ws(" ", F.lit(_cfg.metric_unit), vei.cast("string")))


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


def _other_metrics() -> Column:
    """The notice's color code, alert level, volcano number, notice id and NCEI eruption numbers as JSON.

    Returns:
        ``{"color_code": "ORANGE", "alert_level": "WATCH", "vnum": 311120,
        "vnum_unit": "Smithsonian GVP", "notice_identifier": "DOI-USGS-AVO-2026-09-29T18:26:19+00:00",
        "casualties": 5411, "casualties_unit": "people", "damage": 385.5,
        "damage_unit": "million USD"}`` — each key the feed reports is kept,
        and one it does not report is omitted together with its
        ``<name>_unit`` companion (``ignoreNullFields`` drops the
        ``NULL``). ``color_code`` (the Aviation Color Code, a scale of its
        own — it can move while ``alert_level`` does not) and
        ``alert_level`` both live here because ``danger_rating`` now
        carries the VEI; ``casualties`` (total deaths) and ``damage``
        (total damage, millions of USD) come from the NCEI/WDS eruption
        record and their units travel with them.
    """
    color = F.trim(F.col("color_code"))
    alert = F.trim(F.col("alert_level"))
    identifier = F.trim(F.col("notice_identifier"))
    vnum = F.col("vnum")
    casualties = F.col("casualties")
    damage = F.col("damage")
    return F.to_json(
        F.struct(
            F.when(color != "", color).alias("color_code"),
            F.when(alert != "", alert).alias("alert_level"),
            vnum.alias("vnum"),
            F.when(vnum.isNotNull(), F.lit(_cfg.vnum_unit)).alias("vnum_unit"),
            F.when(identifier != "", identifier).alias("notice_identifier"),
            casualties.alias("casualties"),
            F.when(casualties.isNotNull(), F.lit(_cfg.casualties_unit)).alias("casualties_unit"),
            damage.alias("damage"),
            F.when(damage.isNotNull(), F.lit(_cfg.damage_unit)).alias("damage_unit"),
        )
    )


class UsgsVolcanoEtlScript(SingleEtlScript):
    """USGS volcanic activity notices → ``geo.usgs_volcano_etl`` (canonical event structure)."""

    SOURCE_TABLE = _cfg.source_table
    TARGET_TABLE = _cfg.target_table

    def _convert(self, df: DataFrame) -> DataFrame:
        """Map ``geo.usgs_volcano`` columns onto the canonical ETL structure.

        One row per volcano per notice per snapshot: the feed supplies the
        volcano, observatory, alert level and notice time, the downloader-stamped
        detail columns supply the coordinates, region, status line, full
        notice type and the NCEI eruption numbers (``vei``, ``casualties``,
        ``damage``) — the country is always ``US`` and the subdivision is
        the stamped ``geocode_state`` of the notice point. The metric is the
        VEI (``metric_name`` from the config), and the aviation color code,
        the USGS alert level, the Smithsonian volcano number, the notice
        identifier and the NCEI casualty/damage totals travel in
        :func:`_other_metrics`.

        Args:
            df: Rows read from :attr:`SOURCE_TABLE`.

        Returns:
            The DataFrame in :attr:`TARGET_TABLE` column order.
        """
        return df.select(
            F.lit(_cfg.source_name).alias("source_name"),
            F.col("observatory_name").alias("subsource_name"),
            _event_name().alias("event_name"),
            F.lit(_cfg.event_category).alias("event_category"),
            _event_subcategory().alias("event_subcategory"),
            _description(F.col("synopsis")).alias("event_description"),
            F.col("latitude"),
            F.col("longitude"),
            F.lit(_cfg.country_code).alias("country_code"),
            _state_province(F.col("geocode_state")).alias("state_province"),
            _place_location(F.col("region")).alias("place_location"),
            F.lit(_cfg.metric_name).alias("metric_name"),
            _metric_value().alias("metric_value"),
            F.when(F.col("vei").isNotNull(), F.lit(_cfg.metric_unit)).alias("metric_unit"),
            _danger_rating().alias("danger_rating"),
            _other_metrics().alias("other_metrics"),
            F.col("notice_date").alias("event_start"),
            F.lit(None).cast("timestamp").alias("event_end"),
            F.lit(None).cast("double").alias("last_latitude"),
            F.lit(None).cast("double").alias("last_longitude"),
            F.col(self.FILE_DATE_COL),
        )
