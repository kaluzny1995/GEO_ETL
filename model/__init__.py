from model import raw
from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import JobType
from factory import TableFactory
from model.raw import AvalancheOrgRaw

# Register tables: table_name -> Pydantic model
TableFactory.register("avalanche_org_raw", AvalancheOrgRaw)

__all__ = ["raw", "AppConfig", "SupabaseConnectionConfig", "JobType", "TableFactory"]
