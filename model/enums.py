"""Enumerations used across the GEO_ETL project."""

import enum


class JobType(enum.Enum):
    """Supported ETL job types.

    Attributes:
        CREATE: Creates tables in the target database from Pydantic models.
        INGESTION: Uploads CSV data and transforms raw tables to typed tables.
        DATA_QUALITY: Computes per-file_date quality metrics for a table.
        SINGLE_ETL: Runs one standalone PySpark script from ``single_etl/``.
    """

    CREATE = "CREATE"
    INGESTION = "INGESTION"
    DATA_QUALITY = "DATA_QUALITY"
    SINGLE_ETL = "SINGLE_ETL"


class LoadType(enum.Enum):
    """Data load types supported by the ETL scripts.

    Attributes:
        HISTORICAL: Processes the whole source — first run or full reload.
        INCREMENTAL: Processes only rows newer than the target's latest ``file_date``.
    """

    HISTORICAL = "HISTORICAL"
    INCREMENTAL = "INCREMENTAL"
