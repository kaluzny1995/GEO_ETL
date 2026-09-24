import logging

from pyspark.sql import SparkSession

from model.config import AppConfig, SupabaseConnectionConfig
import utils.raw2plain_utils as _utils


RAW_TABLE = "avalanche_org_raw"
PLAIN_TABLE = "avalanche_org"


def _get_max_file_date(spark, jdbc_url, properties, schema):
    return _utils.get_max_file_date(spark, jdbc_url, properties, schema, PLAIN_TABLE)


def _convert(df):
    F = _utils.F
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


def raw2plain(spark: SparkSession, config: AppConfig, db_config: SupabaseConnectionConfig, logger: logging.Logger | None = None):
    _utils.raw2plain(spark, config, db_config, RAW_TABLE, PLAIN_TABLE, _convert, logger)
