"""Upload OpenMeteo flood CSV files to the typed Supabase table."""

from pyspark.sql import DataFrame

import utils.upload_utils as _utils
from ingestion.upload.upload_script import UploadScript
from model.config.datasets import UploadDatasetConfig, load_dataset

_cfg = load_dataset(__file__, "upload", UploadDatasetConfig)


class OpenMeteoFloodUploadScript(UploadScript):
    """Upload OpenMeteo flood CSV files and convert them to typed columns."""

    TABLE_NAME = _cfg.table_name
    DATASET_FOLDER = _cfg.dataset_folder

    def _convert(self, df: DataFrame) -> DataFrame:
        F = _utils.F
        return df.select(
            F.col("river_name"),
            F.col("country"),
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.to_date("date").alias("date"),
            F.col("discharge_m3s").cast("double"),
            F.col("discharge_mean_m3s").cast("double"),
            F.col("discharge_median_m3s").cast("double"),
            F.col("discharge_p25_m3s").cast("double"),
            F.col("discharge_p75_m3s").cast("double"),
            F.col("geocode_country"),
            F.col("geocode_state"),
            F.col("file_date"),
        )
