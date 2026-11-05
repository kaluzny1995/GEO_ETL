"""Upload GDACS event CSV files to the typed Supabase table."""

from pyspark.sql import DataFrame

import utils.upload_utils as _utils
from ingestion.upload.upload_script import UploadScript
from model.config.datasets import UploadDatasetConfig, load_dataset

_cfg = load_dataset(__file__, "upload", UploadDatasetConfig)


class GdacsEventUploadScript(UploadScript):
    """Upload GDACS event CSV files and convert them to typed columns."""

    TABLE_NAME = _cfg.table_name
    DATASET_FOLDER = _cfg.dataset_folder

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
