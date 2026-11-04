"""DATA_SOURCE_FILE job: publishes one source's CSV inventory to Supabase."""

import logging

from pyspark.sql import SparkSession

from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import LoadType
from utils.data_source_files import publish_source_files


def run(
    spark: SparkSession,
    source_name: str,
    logger: logging.Logger | None = None,
    *,
    load_type: LoadType | None = None,
) -> str:
    """Publish the CSV metadata for one GEO_Download source.

    Args:
        spark: Active Spark session.
        source_name: Source folder name (e.g. ``"EONET"``) — the same name used
            by the DAG's ``DOWNLOAD-<source>`` job.
        logger: Optional logger; defaults to a module-level logger.
        load_type: Force ``HISTORICAL`` or ``INCREMENTAL``. When ``None``, an
            empty metadata table means HISTORICAL and stored rows mean
            INCREMENTAL.

    Returns:
        A human-readable completion message with the load type and row counts.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    app_config = AppConfig.from_json()
    db_config = SupabaseConnectionConfig.from_env()
    result = publish_source_files(
        spark,
        app_config,
        db_config,
        source_name,
        logger=logger,
        load_type=load_type,
    )
    return (
        f"Data source file metadata complete: {result.written_files} written, "
        f"{result.removed_files} removed for {source_name} ({result.load_type.value})"
    )
