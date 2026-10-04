"""Upload NIFC fire CSV files to Supabase."""

from ingestion.upload.upload_script import UploadScript
from model.raw import NifcFireRaw


class NifcFireUploadScript(UploadScript):
    """Upload NIFC fire CSV files to Supabase."""

    TABLE_NAME = "nifc_fire_raw"
    MODEL = NifcFireRaw
    DATASET_FOLDER = "NIFC"
