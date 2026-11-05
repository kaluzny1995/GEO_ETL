"""Upload EONET event CSV files to the typed Supabase table."""

from pyspark.sql import DataFrame

import utils.upload_utils as _utils
from ingestion.upload.upload_script import UploadScript
from model.config.datasets import UploadDatasetConfig, load_dataset

_cfg = load_dataset(__file__, "upload", UploadDatasetConfig)


class EonetEventUploadScript(UploadScript):
    """Upload EONET event CSV files and convert them to typed columns."""

    TABLE_NAME = _cfg.table_name
    DATASET_FOLDER = _cfg.dataset_folder

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
