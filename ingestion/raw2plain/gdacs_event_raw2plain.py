"""Transform GDACS event raw data to typed columns."""

from pyspark.sql import DataFrame

import utils.raw2plain_utils as _utils
from ingestion.raw2plain.raw2plain_script import Raw2PlainScript


class GdacsEventRaw2PlainScript(Raw2PlainScript):
    """Transform GDACS event raw data to typed columns."""

    RAW_TABLE = "gdacs_event_raw"
    PLAIN_TABLE = "gdacs_event"

    def _convert(self, df: DataFrame) -> DataFrame:
        F = _utils.F
        return df.select(
            F.col("event_type"),
            F.col("event_id").cast("int"),
            F.col("event_name"),
            F.col("alert_level"),
            F.col("country"),
            F.col("country_code"),
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.to_timestamp("from_date").alias("from_date"),
            F.to_timestamp("to_date").alias("to_date"),
            F.to_timestamp("date_modified").alias("date_modified"),
            F.col("severity").cast("double"),
            F.col("severity_text"),
            F.col("severity_unit"),
            F.col("source"),
            F.col("report_url"),
            F.when(F.col("is_current").isin("true", "True", "1", "yes"), F.lit(True))
            .otherwise(F.lit(False))
            .alias("is_current"),
            F.col("geocode_country"),
            F.col("geocode_state"),
            F.col("file_date"),
        )
