"""Transform OpenMeteo flood raw data to typed columns."""

from pyspark.sql import DataFrame

import utils.raw2plain_utils as _utils
from ingestion.raw2plain.raw2plain_script import Raw2PlainScript
from model.config.datasets import Raw2PlainDatasetConfig, load_dataset

_cfg = load_dataset(__file__, "raw2plain", Raw2PlainDatasetConfig)


class OpenMeteoFloodRaw2PlainScript(Raw2PlainScript):
    """Transform OpenMeteo flood raw data to typed columns."""

    RAW_TABLE = _cfg.raw_table
    PLAIN_TABLE = _cfg.plain_table

    def _convert(self, df: DataFrame) -> DataFrame:
        F = _utils.F
        return df.select(
            F.col("river_name"),
            F.col("country"),
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.to_date("date").alias("date"),
            F.col("discharge_m3s").cast("double"),
            F.col("geocode_country"),
            F.col("geocode_state"),
            F.col("file_date"),
        )
