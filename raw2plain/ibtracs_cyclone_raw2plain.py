import datetime as dt
import logging
import os

import findspark
findspark.init()

from pyspark.sql import SparkSession, functions as F

from model.config import AppConfig, SupabaseConnectionConfig
from utils.logger import setup_logger

RAW_TABLE = "ibtracs_cyclone_raw"
PLAIN_TABLE = "ibtracs_cyclone"


def _get_max_file_date(spark, jdbc_url: str, properties: dict, schema: str) -> dt.date | None:
    subquery = f"(SELECT MAX(TO_DATE(file_date, 'YYYY-MM-DD')) AS max_file_date FROM {schema}.{PLAIN_TABLE}) AS t"
    try:
        row = spark.read.jdbc(url=jdbc_url, table=subquery, properties=properties).first()
        if row and row.max_file_date:
            return row.max_file_date
    except Exception:
        pass
    return None


def _convert(df):
    return df.select(
        F.col("SID").alias("sid"),
        F.col("SEASON").cast("int").alias("season"),
        F.col("NUMBER").cast("int").alias("number"),
        F.col("BASIN").alias("basin"),
        F.col("SUBBASIN").alias("subbasin"),
        F.col("NAME").alias("name"),
        F.to_timestamp("ISO_TIME").alias("iso_time"),
        F.col("NATURE").alias("nature"),
        F.col("LAT").cast("double").alias("lat"),
        F.col("LON").cast("double").alias("lon"),
        F.col("WMO_WIND").cast("int").alias("wmo_wind"),
        F.col("WMO_PRES").cast("int").alias("wmo_pres"),
        F.col("WMO_AGENCY").alias("wmo_agency"),
        F.col("TRACK_TYPE").alias("track_type"),
        F.col("DIST2LAND").cast("int").alias("dist2land"),
        F.col("LANDFALL").cast("int").alias("landfall"),
        F.col("IFLAG").alias("iflag"),
        F.col("USA_AGENCY").alias("usa_agency"),
        F.col("USA_ATCF_ID").alias("usa_atcf_id"),
        F.col("USA_LAT").cast("double").alias("usa_lat"),
        F.col("USA_LON").cast("double").alias("usa_lon"),
        F.col("USA_RECORD").alias("usa_record"),
        F.col("USA_STATUS").alias("usa_status"),
        F.col("USA_WIND").cast("int").alias("usa_wind"),
        F.col("USA_PRES").cast("int").alias("usa_pres"),
        F.col("USA_SSHS").cast("int").alias("usa_sshs"),
        F.col("USA_R34_NE").cast("int").alias("usa_r34_ne"),
        F.col("USA_R34_SE").cast("int").alias("usa_r34_se"),
        F.col("USA_R34_SW").cast("int").alias("usa_r34_sw"),
        F.col("USA_R34_NW").cast("int").alias("usa_r34_nw"),
        F.col("USA_R50_NE").cast("int").alias("usa_r50_ne"),
        F.col("USA_R50_SE").cast("int").alias("usa_r50_se"),
        F.col("USA_R50_SW").cast("int").alias("usa_r50_sw"),
        F.col("USA_R50_NW").cast("int").alias("usa_r50_nw"),
        F.col("USA_R64_NE").cast("int").alias("usa_r64_ne"),
        F.col("USA_R64_SE").cast("int").alias("usa_r64_se"),
        F.col("USA_R64_SW").cast("int").alias("usa_r64_sw"),
        F.col("USA_R64_NW").cast("int").alias("usa_r64_nw"),
        F.col("USA_POCI").cast("int").alias("usa_poci"),
        F.col("USA_ROCI").cast("int").alias("usa_roci"),
        F.col("USA_RMW").cast("int").alias("usa_rmw"),
        F.col("USA_EYE").cast("int").alias("usa_eye"),
        F.col("STORM_SPEED").cast("int").alias("storm_speed"),
        F.col("STORM_DIR").cast("int").alias("storm_dir"),
        F.col("USA_GUST").cast("int").alias("usa_gust"),
        F.col("file_date"),
    )


def raw2plain(spark: SparkSession, config: AppConfig, db_config: SupabaseConnectionConfig, logger: logging.Logger | None = None):
    schema = config.database.db_schema
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(config.database.jdbc_driver)

    if logger is None:
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
