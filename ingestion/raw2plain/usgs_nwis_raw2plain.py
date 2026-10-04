"""Transform USGS NWIS stream gauge raw data to typed columns."""

from pyspark.sql import DataFrame

import utils.raw2plain_utils as _utils
from ingestion.raw2plain.raw2plain_script import Raw2PlainScript
from model.config.datasets import Raw2PlainDatasetConfig, load_dataset

_cfg = load_dataset(__file__, "raw2plain", Raw2PlainDatasetConfig)


class UsgsNwisRaw2PlainScript(Raw2PlainScript):
    """Transform USGS NWIS stream gauge raw data to typed columns."""

    RAW_TABLE = _cfg.raw_table
    PLAIN_TABLE = _cfg.plain_table

    def _convert(self, df: DataFrame) -> DataFrame:
        F = _utils.F
        return df.select(
            F.col("site_id"),
            F.col("site_name"),
            F.col("parameter_code"),
            F.to_date("date").alias("date"),
            F.col("value").cast("double"),
            F.col("unit"),
            F.col("qualifiers"),
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.col("geocode_state"),
            F.col("file_date"),
        )
