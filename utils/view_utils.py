"""Shared VIEW runner: defines a ``*_vw`` view from a query over a ``*_etl`` table.

Provides a template ``run_view()`` function that proves the dataset's
``SELECT`` query runs and returns rows (via Spark's JDBC reader), then
defines the target view with ``CREATE OR REPLACE VIEW``. Each script in
``views/`` is a :class:`~views.view_script.ViewScript` subclass that supplies
its table/view names and its query from ``config.json``.

Views support the **HISTORICAL** load type only — a view is redefined in full
on every run, there is nothing to load incrementally.
"""

import logging

from pyspark.sql import SparkSession

from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import LoadType
from utils.ddl_generator import execute_statement
from utils.etl_utils import EtlOutcome

LOAD_TYPE = LoadType.HISTORICAL
"""The only load type views support — every run redefines the whole view."""


def run_view(
    spark: SparkSession,
    config: AppConfig,
    db_config: SupabaseConnectionConfig,
    source_table: str,
    target_view: str,
    query: str,
    *,
    logger: logging.Logger | None = None,
) -> EtlOutcome:
    """Run *query* over ``geo.<source_table>`` and define ``geo.<target_view>`` as it.

    The query is executed twice, with two different purposes:

    1. through Spark's JDBC reader — as a ``(<query>) AS t`` subquery — to
       prove it is valid SQL and returns at least one row;
    2. stored as the view body via ``CREATE OR REPLACE VIEW
       geo.<target_view> AS <query>``, so the database recomputes it on every
       read.

    Args:
        spark: Active Spark session.
        config: Application configuration (DB schema, JDBC driver, logging).
        db_config: Supabase connection credentials.
        source_table: ``*_etl`` table the query reads (e.g. ``"union_etl"``).
        target_view: View to create or replace, always ``*_vw``
            (e.g. ``"union_vw"``).
        query: Complete ``SELECT`` statement defining the view; its source
            references should be schema-qualified (e.g. ``geo.union_etl``).
            A trailing ``;`` is tolerated and stripped.
        logger: Optional logger; defaults to this module's logger.

    Returns:
        The :class:`~utils.etl_utils.EtlOutcome` of the run: ``loaded=True``
        with the create report, or ``loaded=False`` when the query returns no
        rows (the view is then left untouched). This runner never raises for
        that load outcome — ``jobs/view.py`` turns ``loaded=False`` into
        ``RuntimeError`` so ``run_etl.py`` exits with code 1, the same
        contract as SINGLE_ETL and INGESTION. (Infrastructure failures and an
        invalid query still surface as exceptions.)
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    schema = config.database.db_schema
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(config.database.jdbc_driver)

    logger.info(f"---- View started: {schema}.{source_table} → {schema}.{target_view} ----")
    logger.info(f"Load type: {LOAD_TYPE.value} (the only load type views support)")

    query = query.strip().rstrip(";").strip()

    logger.info(f"Running the query on {schema}.{source_table}...")
    result_df = spark.read.jdbc(url=jdbc_url, table=f"({query}) AS t", properties=properties)
    if not result_df.head(1):
        return EtlOutcome(False, f"No records loaded into {target_view} — query returned no rows from {source_table}")

    ddl = f"CREATE OR REPLACE VIEW {schema}.{target_view} AS {query};"
    logger.info(f"Defining view {schema}.{target_view}...")
    execute_statement(spark, ddl, jdbc_url, properties)
    logger.info(f"View complete: {schema}.{target_view} defined over {schema}.{source_table}")

    return EtlOutcome(True, f"{LOAD_TYPE.value}: defined view {schema}.{target_view} over {schema}.{source_table}")
