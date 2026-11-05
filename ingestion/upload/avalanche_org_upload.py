"""Upload Avalanche_ORG CSV files to the typed Supabase table."""

from pyspark.sql import DataFrame

import utils.upload_utils as _utils
from ingestion.upload.upload_script import UploadScript
from model.config.datasets import UploadDatasetConfig, load_dataset

_cfg = load_dataset(__file__, "upload", UploadDatasetConfig)


class AvalancheOrgUploadScript(UploadScript):
    """Upload Avalanche_ORG CSV files and convert them to typed columns."""

    TABLE_NAME = _cfg.table_name
    DATASET_FOLDER = _cfg.dataset_folder

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
            F.col("avalanche_size").cast("double"),
            F.to_date("date").alias("date"),
            F.col("file_date"),
        )
