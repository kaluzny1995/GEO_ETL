"""SINGLE_ETL job: runs one standalone PySpark script from ``single_etl/``."""

import logging

from pyspark.sql import SparkSession

from factory import SingleEtlFactory
from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import LoadType


def run(
    spark: SparkSession,
    script_name: str,
    logger: logging.Logger | None = None,
    *,
    load_type: LoadType | None = None,
) -> str:
    """Execute a single ETL script registered in :class:`~factory.SingleEtlFactory`.

    Each registered :class:`~single_etl.single_etl_script.SingleEtlScript` instance runs a
    **HISTORICAL** load (whole source) or an **INCREMENTAL** load (only rows
    newer than the target's latest ``file_date``), detecting the type from its
    target table when *load_type* is ``None`` — the detection and the pipeline
    live in the shared runner ``utils/etl_utils.py``.

    Args:
        spark: Active Spark session.
        script_name: Script name as used on the CLI (file stem of a module in
            ``single_etl/``, e.g. ``"avalanche_org_etl"``).
        logger: Optional logger; defaults to a module-level logger.
        load_type: Load type forced by the CLI (``--load-type``), or ``None``
            for automatic detection inside the script.

    Returns:
        A completion message built from the script's summary.

    Raises:
        ValueError: If no script is registered for *script_name*.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    app_config = AppConfig.from_json()
    db_config = SupabaseConnectionConfig.from_json()

    script = SingleEtlFactory.get(script_name)
    logger.info(f"Running single ETL script: {script_name}...")
    summary = script.run(spark, app_config, db_config, load_type=load_type, logger=logger)
    logger.info(f"Single ETL script {script_name} complete")

    return f"Single ETL {script_name} complete — {summary}"
