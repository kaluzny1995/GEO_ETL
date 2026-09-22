import datetime as dt
import os

import findspark
findspark.init()

from pyspark.sql import SparkSession, functions as F

from model.config import AppConfig, SupabaseConnectionConfig
from utils.logger import setup_logger

RAW_TABLE = "avalanche_org_raw"
PLAIN_TABLE = "avalanche_org"


def _get_max_file_date(spark, jdbc_url: str, properties: dict, schema: str) -> dt.date | None:
    """Query the plain table for the most recent file_date value."""
    subquery = f"(SELECT MAX(TO_DATE(file_date, 'YYYY-MM-DD')) AS max_file_date FROM {schema}.{PLAIN_TABLE}) AS t"
    try:
        row = spark.read.jdbc(url=jdbc_url, table=subquery, properties=properties).first()
        if row and row.max_file_date:
            return row.max_file_date
    except Exception:
        pass
    return None


def _convert(df):
    """Cast raw TEXT columns to final types."""
    return df.select(
        F.col("zone_id").cast("int"),
        F.col("zone_name"),
        F.col("center_id"),
        F.col("center_name"),
        F.col("state"),
        F.col("danger_level").cast("int"),
        F.col("danger_rating"),
        F.col("travel_advice"),
        F.when(F.col("off_season").isin("true", "True", "1", "yes"), F.lit(True))
         .otherwise(F.lit(False))
         .alias("off_season"),
        F.to_timestamp("forecast_start").alias("forecast_start"),
        F.to_timestamp("forecast_end").alias("forecast_end"),
        F.col("warning"),
        F.col("link"),
        F.to_date("date").alias("date"),
        F.col("file_date"),
    )


def raw2plain(spark: SparkSession, config: AppConfig, db_config: SupabaseConnectionConfig):
    """Transform data from avalanche_org_raw to avalanche_org.

    Automatically determines load type:
    - HISTORICAL: plain table empty, processes all raw data
    - INCREMENTAL: plain table has data, processes only raw rows with file_date > max in plain
    """
    schema = config.database.db_schema
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(config.database.jdbc_driver)

    logger = setup_logger(
        name=f"raw2plain_{PLAIN_TABLE}",
        log_file=os.path.join(
            config.logging.directory, "raw2plain",
            f"raw2plain_{PLAIN_TABLE}_{dt.datetime.now().isoformat()}.log",
        ),
        level=config.logging.level_int,
    )
    logger.info(f"---- Raw2Final started: {RAW_TABLE} → {PLAIN_TABLE} ----")

    max_file_date = _get_max_file_date(spark, jdbc_url, properties, schema)

    if max_file_date is None:
        logger.info("Plain table empty → HISTORICAL: loading all raw data")
        raw_df = spark.read.jdbc(url=jdbc_url, table=f"{schema}.{RAW_TABLE}", properties=properties)
    else:
        logger.info(f"Latest file_date in plain: {max_file_date} → INCREMENTAL")
        raw_df = spark.read.jdbc(
            url=jdbc_url,
            table=f"{schema}.{RAW_TABLE}",
            properties=properties,
        ).filter(F.col("file_date") > F.lit(max_file_date.isoformat()))

    record_count = raw_df.count()
    logger.info(f"Records to process: {record_count}")

    if record_count == 0:
        logger.info("No new records. Done.")
        return

    plain_df = _convert(raw_df)
    logger.info(f"Writing to {schema}.{PLAIN_TABLE}...")
    plain_df.write.jdbc(url=jdbc_url, table=f"{schema}.{PLAIN_TABLE}", mode="append", properties=properties)
    logger.info(f"Raw2Plain complete: {record_count} records written to {schema}.{PLAIN_TABLE}")
