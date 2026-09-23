from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import JobType
from model.raw import AvalancheOrgRaw, EonetEventRaw
from model.plain.avalanche_org import AvalancheOrg
from model.plain.eonet_event import EonetEvent


def _register_tables():
    from factory import TableFactory
    TableFactory.register("avalanche_org_raw", AvalancheOrgRaw)
    TableFactory.register("avalanche_org", AvalancheOrg)
    TableFactory.register("eonet_event_raw", EonetEventRaw)
    TableFactory.register("eonet_event", EonetEvent)


_register_tables()

__all__ = ["AppConfig", "SupabaseConnectionConfig", "JobType",
           "AvalancheOrgRaw", "AvalancheOrg", "EonetEventRaw", "EonetEvent"]
