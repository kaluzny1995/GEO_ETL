"""Transform Avalanche_ORG raw data to typed columns."""

from pyspark.sql import DataFrame

import utils.raw2plain_utils as _utils
from ingestion.raw2plain.raw2plain_script import Raw2PlainScript


class AvalancheOrgRaw2PlainScript(Raw2PlainScript):
    """Transform Avalanche_ORG raw data to typed columns."""

    RAW_TABLE = "avalanche_org_raw"
    PLAIN_TABLE = "avalanche_org"

    def _convert(self, df: DataFrame) -> DataFrame:
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
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.to_date("date").alias("date"),
            F.col("file_date"),
        )
