"""Upload USGS volcano CSV files to the typed Supabase table."""

from pyspark.sql import DataFrame

import utils.upload_utils as _utils
from ingestion.upload.upload_script import UploadScript
from model.config.datasets import UploadDatasetConfig, load_dataset

_cfg = load_dataset(__file__, "upload", UploadDatasetConfig)


class UsgsVolcanoUploadScript(UploadScript):
    """Upload USGS volcano CSV files and convert them to typed columns."""

    TABLE_NAME = _cfg.table_name
    DATASET_FOLDER = _cfg.dataset_folder

    def _convert(self, df: DataFrame) -> DataFrame:
        F = _utils.F
        return df.select(
            F.col("volcano_name"),
            F.col("vnum").cast("int"),
            F.col("observatory"),
            F.col("observatory_name"),
            F.col("alert_level"),
            F.col("color_code"),
            F.col("notice_type"),
            F.col("notice_identifier"),
            F.to_timestamp("notice_date").alias("notice_date"),
            F.col("notice_url"),
            F.col("notice_data_url"),
            F.col("notice_type_name"),
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.col("region"),
            F.col("synopsis"),
            F.col("geocode_state"),
            F.col("vei").cast("int"),
            F.col("casualties").cast("int"),
            F.col("damage").cast("double"),
            F.col("file_date"),
        )
