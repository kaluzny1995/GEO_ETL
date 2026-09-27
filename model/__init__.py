from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import JobType
from model.plain.avalanche_org import AvalancheOrg
from model.plain.data_quality import DataQuality
from model.plain.eonet_event import EonetEvent
from model.plain.gdacs_event import GdacsEvent
from model.plain.ibtracs_cyclone import IbtacsCyclone
from model.plain.nifc_fire import NifcFire
from model.plain.openmeteo_flood import OpenMeteoFlood
from model.plain.usgs_earthquake import UsgsEarthquake
from model.plain.usgs_nwis import UsgsNwis
from model.plain.usgs_volcano import UsgsVolcano
from model.raw import (
    AvalancheOrgRaw,
    EonetEventRaw,
    GdacsEventRaw,
    IbtacsCycloneRaw,
    NifcFireRaw,
    OpenMeteoFloodRaw,
    UsgsEarthquakeRaw,
    UsgsNwisRaw,
    UsgsVolcanoRaw,
)


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
    TableFactory.register("nifc_fire_raw", NifcFireRaw)
    TableFactory.register("nifc_fire", NifcFire)
    TableFactory.register("openmeteo_flood_raw", OpenMeteoFloodRaw)
    TableFactory.register("openmeteo_flood", OpenMeteoFlood)
    TableFactory.register("usgs_earthquake_raw", UsgsEarthquakeRaw)
    TableFactory.register("usgs_earthquake", UsgsEarthquake)
    TableFactory.register("usgs_nwis_raw", UsgsNwisRaw)
    TableFactory.register("usgs_nwis", UsgsNwis)
    TableFactory.register("usgs_volcano_raw", UsgsVolcanoRaw)
    TableFactory.register("usgs_volcano", UsgsVolcano)
    TableFactory.register("data_quality", DataQuality)


_register_tables()

__all__ = [
    "AppConfig",
    "AvalancheOrg",
    "AvalancheOrgRaw",
    "DataQuality",
    "EonetEvent",
    "EonetEventRaw",
    "GdacsEvent",
    "GdacsEventRaw",
    "IbtacsCyclone",
    "IbtacsCycloneRaw",
    "JobType",
    "NifcFire",
    "NifcFireRaw",
    "OpenMeteoFlood",
    "OpenMeteoFloodRaw",
    "SupabaseConnectionConfig",
    "UsgsEarthquake",
    "UsgsEarthquakeRaw",
    "UsgsNwis",
    "UsgsNwisRaw",
    "UsgsVolcano",
    "UsgsVolcanoRaw",
]
