"""Upload USGS earthquake CSV files to Supabase."""

from ingestion.upload.upload_script import UploadScript
from model.config.datasets import UploadDatasetConfig, load_dataset

_cfg = load_dataset(__file__, "upload", UploadDatasetConfig)


class UsgsEarthquakeUploadScript(UploadScript):
    """Upload USGS earthquake CSV files to Supabase."""

    TABLE_NAME = _cfg.table_name
    DATASET_FOLDER = _cfg.dataset_folder
