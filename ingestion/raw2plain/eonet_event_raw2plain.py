"""Transform EONET event raw data to typed columns."""

from pyspark.sql import DataFrame

import utils.raw2plain_utils as _utils
from ingestion.raw2plain.raw2plain_script import Raw2PlainScript
from model.config.datasets import Raw2PlainDatasetConfig, load_dataset

_cfg = load_dataset(__file__, "raw2plain", Raw2PlainDatasetConfig)


class EonetEventRaw2PlainScript(Raw2PlainScript):
    """Transform EONET event raw data to typed columns."""

    RAW_TABLE = _cfg.raw_table
    PLAIN_TABLE = _cfg.plain_table

    def _convert(self, df: DataFrame) -> DataFrame:
        F = _utils.F
        return df.select(
            F.col("event_id"),
            F.col("event_name"),
            F.col("category_id"),
            F.col("category_name"),
            F.col("description"),
            F.col("source_id"),
            F.col("source_url"),
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.to_timestamp("event_date").alias("event_date"),
            F.col("magnitude_value").cast("double"),
            F.col("magnitude_unit"),
            F.to_date("closed").alias("closed"),
            F.col("geometry_count").cast("int"),
            F.col("last_latitude").cast("double"),
            F.col("last_longitude").cast("double"),
            F.to_timestamp("last_date").alias("last_date"),
            F.col("geocode_country"),
            F.col("geocode_state"),
            F.col("file_date"),
        )
