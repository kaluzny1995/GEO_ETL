"""Template for a standalone single-table PySpark ETL script.

**Turn this into a real script**

1. Copy the file inside ``single_etl/`` and rename it after your dataset,
   e.g. ``my_dataset_etl.py``.
2. Set :data:`SOURCE_TABLE` / :data:`TARGET_TABLE` and fill in
   :func:`_convert` (the dataset-specific transformation).
3. Register the script in ``jobs/__init__.py``::

       from single_etl.my_dataset_etl import run as my_dataset_etl
       SingleEtlFactory.register("my_dataset_etl", my_dataset_etl)

4. Run it::

       python run_etl.py SINGLE_ETL my_dataset_etl
       python run_etl.py SINGLE_ETL my_dataset_etl --load-type historical
       python run_etl.py SINGLE_ETL my_dataset_etl --load-type incremental

**Load types** (resolved by :func:`_resolve_load_type`)

- ``HISTORICAL`` — every row of ``SOURCE_TABLE`` is processed (first run, or a
  full reprocess forced with ``--load-type historical``; clear the target first
  if the write must not duplicate rows).
- ``INCREMENTAL`` — only rows whose ``file_date`` is newer than the latest
  ``file_date`` stored in ``TARGET_TABLE``.

``--load-type auto`` (the default) leaves the decision to the script: an empty
target (or a target table that does not exist yet) means ``HISTORICAL``,
otherwise ``INCREMENTAL``. Both types append to the target.
"""

import datetime as dt
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import LoadType
from utils.raw2plain_utils import get_max_file_date

SOURCE_TABLE = "example_source"
TARGET_TABLE = "example_target"
FILE_DATE_COL = "file_date"


def _convert(df: DataFrame) -> DataFrame:
    """Apply the dataset-specific transformation.

    Replace the passthrough below with casts, renames, joins or filters — the
    same role as the ``_convert()`` functions in ``ingestion/raw2plain/``.

    Args:
        df: Source rows read from :data:`SOURCE_TABLE`.

    Returns:
        The transformed DataFrame ready to be appended to :data:`TARGET_TABLE`.
    """
    return df


def _resolve_load_type(max_file_date: dt.date | None, requested: LoadType | None) -> LoadType:
    """Decide which load type a run should use.

    Args:
        max_file_date: Latest ``file_date`` stored in the target table, or
            ``None`` when the target is empty or does not exist yet.
        requested: Load type forced on the CLI (``--load-type``), or ``None``
            for automatic detection.

    Returns:
        The load type to run with:

        * empty target → ``HISTORICAL`` (there is nothing to be incremental from),
        * explicit request → the requested type (``HISTORICAL`` forces a full reprocess),
        * otherwise → ``INCREMENTAL``.
    """
    if max_file_date is None:
        return LoadType.HISTORICAL
    if requested is not None:
        return requested
    return LoadType.INCREMENTAL


def run(
    spark: SparkSession,
    config: AppConfig,
    db_config: SupabaseConnectionConfig,
    *,
    load_type: LoadType | None = None,
    logger: logging.Logger | None = None,
) -> str:
    """Read ``SOURCE_TABLE``, transform it and append the result to ``TARGET_TABLE``.

    Args:
        spark: Active Spark session.
        config: Application configuration (DB schema, JDBC driver, logging).
        db_config: Supabase connection credentials.
        load_type: Load type forced by the caller, or ``None`` to detect it
            from the target table.
        logger: Optional logger; defaults to the module logger.

    Returns:
        A human-readable summary, e.g. ``"INCREMENTAL: wrote to geo.example_target"``
        or ``"INCREMENTAL: nothing to do — example_target is up to date"``.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    schema = config.database.db_schema
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(config.database.jdbc_driver)

    logger.info(f"---- Single ETL started: {schema}.{SOURCE_TABLE} → {schema}.{TARGET_TABLE} ----")

    max_file_date = get_max_file_date(spark, jdbc_url, properties, schema, TARGET_TABLE)
    resolved = _resolve_load_type(max_file_date, load_type)

    if load_type is None:
        reason = "target table empty" if max_file_date is None else f"latest file_date in target: {max_file_date}"
        logger.info(f"Load type auto-detected ({reason}) → {resolved.value}")
    elif resolved is not load_type:
        logger.info(f"{load_type.value} requested but the target table is empty → {resolved.value}")
    else:
        logger.info(f"Load type requested: {resolved.value}")

    source_df = spark.read.jdbc(url=jdbc_url, table=f"{schema}.{SOURCE_TABLE}", properties=properties)

    if resolved is LoadType.INCREMENTAL and max_file_date is not None:
        logger.info(f"Processing rows of {SOURCE_TABLE} with {FILE_DATE_COL} > {max_file_date}...")
        source_df = source_df.filter(F.col(FILE_DATE_COL) > F.lit(max_file_date.isoformat()))
    else:
        logger.info(f"Processing every row of {SOURCE_TABLE}...")

    if not source_df.head(1):
        logger.warning(f"No new records for {TARGET_TABLE} — nothing to do, table is up to date")
        return f"{resolved.value}: nothing to do — {TARGET_TABLE} is up to date"

    result_df = _convert(source_df)
    logger.info(f"Writing to {schema}.{TARGET_TABLE}...")
    result_df.write.jdbc(url=jdbc_url, table=f"{schema}.{TARGET_TABLE}", mode="append", properties=properties)
    logger.info(f"Single ETL complete: written to {schema}.{TARGET_TABLE}")
    return f"{resolved.value}: wrote to {schema}.{TARGET_TABLE}"
