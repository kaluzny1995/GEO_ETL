"""Upload IBTrACS cyclone CSV files to Supabase."""

from ingestion.upload.upload_script import UploadScript
from model.raw import IbtacsCycloneRaw


class IbtracsCycloneUploadScript(UploadScript):
    """Upload IBTrACS cyclone CSV files to Supabase."""

    TABLE_NAME = "ibtracs_cyclone_raw"
    MODEL = IbtacsCycloneRaw
    DATASET_FOLDER = "IBTrACS"
