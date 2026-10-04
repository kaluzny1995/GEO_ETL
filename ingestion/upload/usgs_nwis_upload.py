"""Upload USGS NWIS stream gauge CSV files to Supabase."""

from ingestion.upload.upload_script import UploadScript
from model.raw import UsgsNwisRaw


class UsgsNwisUploadScript(UploadScript):
    """Upload USGS NWIS stream gauge CSV files to Supabase."""

    TABLE_NAME = "usgs_nwis_raw"
    MODEL = UsgsNwisRaw
    DATASET_FOLDER = "USGS_NWIS"
