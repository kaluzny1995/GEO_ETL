"""VIEW job: runs one standalone PySpark script from ``views/``."""

import logging

from pyspark.sql import SparkSession

from factory import ViewScriptFactory
from model.config import AppConfig, SupabaseConnectionConfig


def run(spark: SparkSession, view_name: str, logger: logging.Logger | None = None) -> str:
    """Execute a view script registered in :class:`~factory.ViewScriptFactory`.

    Each registered :class:`~views.view_script.ViewScript` reads its
    ``*_etl`` table through Spark, proves its query returns rows and defines
    the ``*_vw`` view with ``CREATE OR REPLACE VIEW``. The pipeline is always
    a **HISTORICAL** load — views are redefined in full on every run, there
    is no incremental view load (so ``--load-type`` is rejected for this job
    on the CLI).

    Args:
        spark: Active Spark session.
        view_name: View name as used on the CLI (file stem of a module in
            ``views/``, e.g. ``"union_vw"``).
        logger: Optional logger; defaults to a module-level logger.

    Returns:
        A completion message built from the script's summary.

    Raises:
        ValueError: If no script is registered for *view_name*.
        RuntimeError: When the script's query returned no rows (its
            :class:`~utils.etl_utils.EtlOutcome` reports ``loaded=False``) —
            the job warns with that summary and raises, so ``run_etl.py``
            exits with code 1 (same contract as SINGLE_ETL).
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    app_config = AppConfig.from_json()
    db_config = SupabaseConnectionConfig.from_env()

    script = ViewScriptFactory.get(view_name)
    logger.info(f"Running view script: {view_name}...")
    outcome = script.run(spark, app_config, db_config, logger=logger)
    if not outcome.loaded:
        logger.warning(outcome.summary)
        raise RuntimeError(outcome.summary)
    logger.info(f"View script {view_name} complete")

    return f"View {view_name} complete — {outcome.summary}"
