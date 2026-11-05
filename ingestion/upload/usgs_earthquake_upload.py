"""Upload USGS earthquake CSV files to the typed Supabase table."""

from pyspark.sql import DataFrame

import utils.upload_utils as _utils
from ingestion.upload.upload_script import UploadScript
from model.config.datasets import UploadDatasetConfig, load_dataset

_cfg = load_dataset(__file__, "upload", UploadDatasetConfig)


class UsgsEarthquakeUploadScript(UploadScript):
    """Upload USGS earthquake CSV files and convert them to typed columns."""

    TABLE_NAME = _cfg.table_name
    DATASET_FOLDER = _cfg.dataset_folder

    def _convert(self, df: DataFrame) -> DataFrame:
        F = _utils.F
        return df.select(
            F.to_timestamp("time").alias("time"),
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.col("depth").cast("double"),
            F.col("mag").cast("double"),
            F.col("magType").alias("mag_type"),
            F.col("nst").cast("int"),
            F.col("gap").cast("int"),
            F.col("dmin").cast("double"),
            F.col("rms").cast("double"),
            F.col("net"),
            F.col("id").alias("event_id"),
            F.to_timestamp("updated").alias("updated"),
            F.col("place"),
            F.col("type").alias("event_type"),
            F.col("horizontalError").cast("double").alias("horizontal_error"),
            F.col("depthError").cast("double").alias("depth_error"),
            F.col("magError").cast("double").alias("mag_error"),
            F.col("magNst").cast("int").alias("mag_nst"),
            F.col("status"),
            F.col("locationSource").alias("location_source"),
            F.col("magSource").alias("mag_source"),
            F.col("geocode_country"),
            F.col("geocode_state"),
            F.col("file_date"),
        )
