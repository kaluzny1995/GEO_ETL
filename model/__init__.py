from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import JobType
from model.raw import AvalancheOrgRaw, EonetEventRaw, GdacsEventRaw, IbtacsCycloneRaw
from model.plain.avalanche_org import AvalancheOrg
from model.plain.eonet_event import EonetEvent
from model.plain.gdacs_event import GdacsEvent
from model.plain.ibtracs_cyclone import IbtacsCyclone


def _register_tables():
    from factory import TableFactory
    TableFactory.register("avalanche_org_raw", AvalancheOrgRaw)
    TableFactory.register("avalanche_org", AvalancheOrg)
    TableFactory.register("eonet_event_raw", EonetEventRaw)
    TableFactory.register("eonet_event", EonetEvent)
    TableFactory.register("gdacs_event_raw", GdacsEventRaw)
    TableFactory.register("gdacs_event", GdacsEvent)
    TableFactory.register("ibtracs_cyclone_raw", IbtacsCycloneRaw)
    TableFactory.register("ibtracs_cyclone", IbtacsCyclone)


_register_tables()

__all__ = ["AppConfig", "SupabaseConnectionConfig", "JobType",
           "AvalancheOrgRaw", "AvalancheOrg", "EonetEventRaw", "EonetEvent",
           "GdacsEventRaw", "GdacsEvent", "IbtacsCycloneRaw", "IbtacsCyclone"]
