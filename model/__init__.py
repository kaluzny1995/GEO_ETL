from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import JobType
from model.raw import AvalancheOrgRaw
from model.plain.avalanche_org import AvalancheOrg


def _register_tables():
    from factory import TableFactory
    TableFactory.register("avalanche_org_raw", AvalancheOrgRaw)
    TableFactory.register("avalanche_org", AvalancheOrg)


_register_tables()

__all__ = ["AppConfig", "SupabaseConnectionConfig", "JobType", "AvalancheOrgRaw", "AvalancheOrg"]
