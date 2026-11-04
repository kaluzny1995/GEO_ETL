"""Metadata contracts for the CSV snapshots published by the DATA_SOURCE_FILE job."""

from pydantic import BaseModel


class DataSourceFile(BaseModel):
    """One CSV snapshot in the GEO_Download data folder.

    Attributes:
        source_name: Source folder name (e.g. ``"EONET"``).
        file_name: CSV file name.
        snapshot_date: ``YYYY-MM-DD`` parsed from the file name, if any.
        created_at: Filesystem modification time as ISO-8601 UTC text.
        records: Data rows excluding the header and blank lines.
        size_kb: File size in 1024-byte kilobytes, rounded to one decimal.
        pipeline_start_date: Earliest enabled source start date from
            GEO_Download's ``config.json`` at publication time.
    """

    source_name: str
    file_name: str
    snapshot_date: str | None = None
    created_at: str
    records: int
    size_kb: float
    pipeline_start_date: str | None = None


class DataSourceSource(BaseModel):
    """One published source folder, kept separately from its CSV rows.

    This table keeps a source visible in GEO_API's dropdown even when the
    folder currently has no CSV files; ``geo.data_source_files`` therefore
    contains real files only.

    Attributes:
        source_name: Source folder name (e.g. ``"EONET"``).
        pipeline_start_date: Earliest enabled source start date from
            GEO_Download's ``config.json``.
        updated_at: When the source row was last refreshed (ISO-8601 UTC).
    """

    source_name: str
    pipeline_start_date: str | None = None
    updated_at: str
