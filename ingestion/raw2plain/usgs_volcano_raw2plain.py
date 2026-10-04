"""Transform USGS volcano raw data to typed columns."""

from pyspark.sql import DataFrame

import utils.raw2plain_utils as _utils
from ingestion.raw2plain.raw2plain_script import Raw2PlainScript


class UsgsVolcanoRaw2PlainScript(Raw2PlainScript):
    """Transform USGS volcano raw data to typed columns."""

    RAW_TABLE = "usgs_volcano_raw"
    PLAIN_TABLE = "usgs_volcano"

    def _convert(self, df: DataFrame) -> DataFrame:
        F = _utils.F
        return df.select(
            F.col("volcano_name"),
            F.col("vnum").cast("int"),
            F.col("observatory"),
            F.col("observatory_name"),
            F.col("alert_level"),
            F.col("color_code"),
            F.col("notice_type"),
            F.col("notice_identifier"),
            F.to_timestamp("notice_date").alias("notice_date"),
            F.col("notice_url"),
            F.col("notice_data_url"),
            F.col("notice_type_name"),
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.col("region"),
            F.col("synopsis"),
            F.col("geocode_state"),
            F.col("file_date"),
        )
