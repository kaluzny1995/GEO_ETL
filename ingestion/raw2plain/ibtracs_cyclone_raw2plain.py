"""Transform IBTrACS cyclone raw data to typed columns."""

import logging

from pyspark.sql import SparkSession

import utils.raw2plain_utils as _utils
from model.config import AppConfig, SupabaseConnectionConfig

RAW_TABLE = "ibtracs_cyclone_raw"
PLAIN_TABLE = "ibtracs_cyclone"


def _get_max_file_date(spark, jdbc_url, properties, schema):
    return _utils.get_max_file_date(spark, jdbc_url, properties, schema, PLAIN_TABLE)


def _convert(df):
    F = _utils.F
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


def raw2plain(
    spark: SparkSession, config: AppConfig, db_config: SupabaseConnectionConfig, logger: logging.Logger | None = None
):
    """Transform raw data to typed columns for this data source.

    Delegates to :func:`utils.raw2plain_utils.raw2plain` with the
    dataset-specific convert function.

    Args:
        spark: Active Spark session.
        config: Application configuration.
        db_config: Supabase connection credentials.
        logger: Optional logger for consolidated logging.
    """
    return _utils.raw2plain(spark, config, db_config, RAW_TABLE, PLAIN_TABLE, _convert, logger)
