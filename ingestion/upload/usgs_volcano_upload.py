"""Upload USGS volcano CSV files to Supabase."""

from ingestion.upload.upload_script import UploadScript
from model.raw import UsgsVolcanoRaw


class UsgsVolcanoUploadScript(UploadScript):
    """Upload USGS volcano CSV files to Supabase."""

    TABLE_NAME = "usgs_volcano_raw"
    MODEL = UsgsVolcanoRaw
    DATASET_FOLDER = "USGS_Volcano"
