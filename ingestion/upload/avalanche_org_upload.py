"""Upload Avalanche_ORG CSV files to Supabase."""

from ingestion.upload.upload_script import UploadScript
from model.raw import AvalancheOrgRaw


class AvalancheOrgUploadScript(UploadScript):
    """Upload Avalanche_ORG CSV files to Supabase."""

    TABLE_NAME = "avalanche_org_raw"
    MODEL = AvalancheOrgRaw
    DATASET_FOLDER = "Avalanche_ORG"
