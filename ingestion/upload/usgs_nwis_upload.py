"""Upload USGS NWIS stream gauge CSV files to the typed Supabase table."""

from pyspark.sql import DataFrame

import utils.upload_utils as _utils
from ingestion.upload.upload_script import UploadScript
from model.config.datasets import UploadDatasetConfig, load_dataset

_cfg = load_dataset(__file__, "upload", UploadDatasetConfig)


class UsgsNwisUploadScript(UploadScript):
    """Upload USGS NWIS stream gauge CSV files and convert them to typed columns."""

    TABLE_NAME = _cfg.table_name
    DATASET_FOLDER = _cfg.dataset_folder

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
            F.col("gauge_height").cast("double"),
            F.col("flood_category"),
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.col("geocode_state"),
            F.col("file_date"),
        )
