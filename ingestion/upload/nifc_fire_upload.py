"""Upload NIFC fire CSV files to the typed Supabase table."""

from pyspark.sql import DataFrame

import utils.upload_utils as _utils
from ingestion.upload.upload_script import UploadScript
from model.config.datasets import UploadDatasetConfig, load_dataset

_cfg = load_dataset(__file__, "upload", UploadDatasetConfig)


class NifcFireUploadScript(UploadScript):
    """Upload NIFC fire CSV files and convert them to typed columns."""

    TABLE_NAME = _cfg.table_name
    DATASET_FOLDER = _cfg.dataset_folder

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
