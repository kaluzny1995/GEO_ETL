"""Upload EONET event CSV files to Supabase."""

from ingestion.upload.upload_script import UploadScript
from model.raw import EonetEventRaw


class EonetEventUploadScript(UploadScript):
    """Upload EONET event CSV files to Supabase."""

    TABLE_NAME = "eonet_event_raw"
    MODEL = EonetEventRaw
    DATASET_FOLDER = "EONET"
