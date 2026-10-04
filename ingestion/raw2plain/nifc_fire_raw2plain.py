"""Transform NIFC fire raw data to typed columns."""

from pyspark.sql import DataFrame

import utils.raw2plain_utils as _utils
from ingestion.raw2plain.raw2plain_script import Raw2PlainScript
from model.config.datasets import Raw2PlainDatasetConfig, load_dataset

_cfg = load_dataset(__file__, "raw2plain", Raw2PlainDatasetConfig)


class NifcFireRaw2PlainScript(Raw2PlainScript):
    """Transform NIFC fire raw data to typed columns."""

    RAW_TABLE = _cfg.raw_table
    PLAIN_TABLE = _cfg.plain_table

    def _convert(self, df: DataFrame) -> DataFrame:
        F = _utils.F
        return df.select(
            F.col("incident_name"),
            F.col("irwin_id"),
            F.col("fire_number"),
            F.col("acres").cast("double"),
            F.col("percent_contained").cast("double"),
            F.col("fire_type"),
            F.col("fire_type_label"),
            F.col("jurisdiction"),
            F.col("jurisdiction_label"),
            F.col("unit_name"),
            F.col("state"),
            F.col("po_method"),
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.to_timestamp(F.col("poly_date") / 1000).alias("poly_date"),
            F.to_timestamp(F.col("created_on") / 1000).alias("created_on"),
            F.to_timestamp(F.col("modified_on") / 1000).alias("modified_on"),
            F.col("geocode_state"),
            F.col("file_date"),
        )
