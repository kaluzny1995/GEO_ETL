"""Upload OpenMeteo flood CSV files to Supabase."""

from ingestion.upload.upload_script import UploadScript
from model.raw import OpenMeteoFloodRaw


class OpenMeteoFloodUploadScript(UploadScript):
    """Upload OpenMeteo flood CSV files to Supabase."""

    TABLE_NAME = "openmeteo_flood_raw"
    MODEL = OpenMeteoFloodRaw
    DATASET_FOLDER = "OpenMeteo_Flood"
