"""Transform OpenMeteo flood raw data to typed columns."""

from pyspark.sql import DataFrame

import utils.raw2plain_utils as _utils
from ingestion.raw2plain.raw2plain_script import Raw2PlainScript


class OpenMeteoFloodRaw2PlainScript(Raw2PlainScript):
    """Transform OpenMeteo flood raw data to typed columns."""

    RAW_TABLE = "openmeteo_flood_raw"
    PLAIN_TABLE = "openmeteo_flood"

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
