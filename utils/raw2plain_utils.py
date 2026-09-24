import datetime as dt
import logging
import os

import findspark
findspark.init()

from pyspark.sql import SparkSession, functions as F

from model.config import AppConfig, SupabaseConnectionConfig
from utils.logger import setup_logger


def get_max_file_date(spark, jdbc_url: str, properties: dict, schema: str, table_name: str) -> dt.date | None:
    subquery = f"(SELECT MAX(TO_DATE(file_date, 'YYYY-MM-DD')) AS max_file_date FROM {schema}.{table_name}) AS t"
    try:
        row = spark.read.jdbc(url=jdbc_url, table=subquery, properties=properties).first()
        if row and row.max_file_date:
            return row.max_file_date
    except Exception:
        pass
    return None


def raw2plain(spark: SparkSession, config: AppConfig, db_config: SupabaseConnectionConfig,
              raw_table: str, plain_table: str, convert_fn, logger: logging.Logger | None = None):
    schema = config.database.db_schema
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(config.database.jdbc_driver)

    if logger is None:
        logger = setup_logger(
            name=f"raw2plain_{plain_table}",
            log_file=os.path.join(
                config.logging.directory, "raw2plain",
                f"raw2plain_{plain_table}_{dt.datetime.now().isoformat()}.log",
            ),
            level=config.logging.level_int,
        )
    logger.info(f"---- Raw2Final started: {raw_table} → {plain_table} ----")

    max_file_date = get_max_file_date(spark, jdbc_url, properties, schema, plain_table)

    if max_file_date is None:
        logger.info("Plain table empty → HISTORICAL: loading all raw data")
        raw_df = spark.read.jdbc(url=jdbc_url, table=f"{schema}.{raw_table}", properties=properties)
    else:
        logger.info(f"Latest file_date in plain: {max_file_date} → INCREMENTAL")
        raw_df = spark.read.jdbc(
            url=jdbc_url,
            table=f"{schema}.{raw_table}",
            properties=properties,
        ).filter(F.col("file_date") > F.lit(max_file_date.isoformat()))

    if not raw_df.head(1):
        logger.warning(f"No new records to process for {plain_table}")
        raise ValueError(f"No new records to process for {plain_table}")

    plain_df = convert_fn(raw_df)
    logger.info(f"Writing to {schema}.{plain_table}...")
    plain_df.write.jdbc(url=jdbc_url, table=f"{schema}.{plain_table}", mode="append", properties=properties)
    logger.info(f"Raw2Plain complete: written to {schema}.{plain_table}")
