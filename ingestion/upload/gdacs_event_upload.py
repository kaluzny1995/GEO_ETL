"""Upload GDACS event CSV files to Supabase."""

from ingestion.upload.upload_script import UploadScript
from model.raw import GdacsEventRaw


class GdacsEventUploadScript(UploadScript):
    """Upload GDACS event CSV files to Supabase."""

    TABLE_NAME = "gdacs_event_raw"
    MODEL = GdacsEventRaw
    DATASET_FOLDER = "GDACS"
