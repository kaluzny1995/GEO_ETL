"""Transform USGS volcano raw data to typed columns."""

from pyspark.sql import DataFrame

import utils.raw2plain_utils as _utils
from ingestion.raw2plain.raw2plain_script import Raw2PlainScript
from model.config.datasets import Raw2PlainDatasetConfig, load_dataset

_cfg = load_dataset(__file__, "raw2plain", Raw2PlainDatasetConfig)


class UsgsVolcanoRaw2PlainScript(Raw2PlainScript):
    """Transform USGS volcano raw data to typed columns."""

    RAW_TABLE = _cfg.raw_table
    PLAIN_TABLE = _cfg.plain_table

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
