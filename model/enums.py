"""Enumerations used across the GEO_ETL project."""

import enum


class JobType(enum.Enum):
    """Supported ETL job types.

    Attributes:
        CREATE: Creates tables in the target database from Pydantic models.
        INGESTION: Uploads CSV data and transforms raw tables to typed tables.
        DATA_QUALITY: Computes per-file_date quality metrics for a table.
        SINGLE_ETL: Runs one standalone PySpark script from ``single_etl/``.
        VIEW: Runs one standalone PySpark script from ``views/`` that creates a
            ``*_vw`` view from a query over a ``*_etl`` table (HISTORICAL only).
    """

    CREATE = "CREATE"
    INGESTION = "INGESTION"
    DATA_QUALITY = "DATA_QUALITY"
    SINGLE_ETL = "SINGLE_ETL"
    VIEW = "VIEW"


class LoadType(enum.Enum):
    """Data load types supported by the ETL scripts.

    Attributes:
        HISTORICAL: Processes the whole source — first run or full reload.
        INCREMENTAL: Processes only rows newer than the target's latest ``file_date``.
    """

    HISTORICAL = "HISTORICAL"
    INCREMENTAL = "INCREMENTAL"


class DangerRating(enum.StrEnum):
    """Harmonized severity scale every ``*_etl`` script writes into ``danger_rating``.

    The nine sources report their severity on their own scales (NAPADS levels,
    GDACS alert levels, Saffir-Simpson classes, discharge percentiles, NWS flood
    categories, USGS alert levels); each script maps its native scale onto these
    words, so ``geo.union_etl`` (and every ``*_etl`` table) carries one vocabulary
    that can be compared across sources. It is a ``StrEnum`` because the words are
    used as data — they travel through ``config.json``, Spark literals and the
    ``danger_rating`` column itself.

    Attributes:
        NO_RATING: The source cannot rate the row (no rating product, unrated
            off-season zone, missing reference, unknown code or a rating that
            reports no danger) — the value that used to be ``NULL``.
        LOW: Minor danger.
        MEDIUM: Moderate danger.
        HIGH: Serious danger.
        EXTREME: The most severe class the scale reserves for catastrophic conditions.
    """

    NO_RATING = "no_rating"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    EXTREME = "extreme"
