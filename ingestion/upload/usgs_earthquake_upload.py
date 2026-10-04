"""Upload USGS earthquake CSV files to Supabase."""

from ingestion.upload.upload_script import UploadScript
from model.raw import UsgsEarthquakeRaw


class UsgsEarthquakeUploadScript(UploadScript):
    """Upload USGS earthquake CSV files to Supabase."""

    TABLE_NAME = "usgs_earthquake_raw"
    MODEL = UsgsEarthquakeRaw
    DATASET_FOLDER = "USGS_Earthquake"
