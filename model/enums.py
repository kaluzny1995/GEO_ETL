"""Enumerations used across the GEO_ETL project."""

import enum


class JobType(enum.Enum):
    """Supported ETL job types.

    Attributes:
        CREATE: Creates tables in the target database from Pydantic models.
        INGESTION: Uploads CSV data and transforms raw tables to typed tables.
    """
    CREATE = "CREATE"
    INGESTION = "INGESTION"
