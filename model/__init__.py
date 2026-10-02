from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import JobType
from model.etl.avalanche_org_etl import AvalancheOrgEtl
from model.etl.eonet_event_etl import EonetEventEtl
from model.etl.gdacs_event_etl import GdacsEventEtl
from model.etl.ibtracs_cyclone_etl import IbtracsCycloneEtl
from model.etl.nifc_fire_etl import NifcFireEtl
from model.etl.openmeteo_flood_etl import OpenMeteoFloodEtl
from model.etl.usgs_earthquake_etl import UsgsEarthquakeEtl
from model.other.data_quality import DataQuality
from model.plain.avalanche_org import AvalancheOrg
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
    TableFactory.register("avalanche_org_etl", AvalancheOrgEtl)
    TableFactory.register("eonet_event_raw", EonetEventRaw)
    TableFactory.register("eonet_event", EonetEvent)
    TableFactory.register("eonet_event_etl", EonetEventEtl)
    TableFactory.register("gdacs_event_raw", GdacsEventRaw)
    TableFactory.register("gdacs_event", GdacsEvent)
    TableFactory.register("gdacs_event_etl", GdacsEventEtl)
    TableFactory.register("ibtracs_cyclone_raw", IbtacsCycloneRaw)
    TableFactory.register("ibtracs_cyclone", IbtacsCyclone)
    TableFactory.register("ibtracs_cyclone_etl", IbtracsCycloneEtl)
    TableFactory.register("nifc_fire_raw", NifcFireRaw)
    TableFactory.register("nifc_fire", NifcFire)
    TableFactory.register("nifc_fire_etl", NifcFireEtl)
    TableFactory.register("openmeteo_flood_raw", OpenMeteoFloodRaw)
    TableFactory.register("openmeteo_flood", OpenMeteoFlood)
    TableFactory.register("openmeteo_flood_etl", OpenMeteoFloodEtl)
    TableFactory.register("usgs_earthquake_raw", UsgsEarthquakeRaw)
    TableFactory.register("usgs_earthquake", UsgsEarthquake)
    TableFactory.register("usgs_earthquake_etl", UsgsEarthquakeEtl)
    TableFactory.register("usgs_nwis_raw", UsgsNwisRaw)
    TableFactory.register("usgs_nwis", UsgsNwis)
    TableFactory.register("usgs_volcano_raw", UsgsVolcanoRaw)
    TableFactory.register("usgs_volcano", UsgsVolcano)
    TableFactory.register("data_quality", DataQuality)


_register_tables()

__all__ = [
    "AppConfig",
    "AvalancheOrg",
    "AvalancheOrgEtl",
    "AvalancheOrgRaw",
    "DataQuality",
    "EonetEvent",
    "EonetEventEtl",
    "EonetEventRaw",
    "GdacsEvent",
    "GdacsEventEtl",
    "GdacsEventRaw",
    "IbtacsCyclone",
    "IbtacsCycloneRaw",
    "IbtracsCycloneEtl",
    "JobType",
    "NifcFire",
    "NifcFireEtl",
    "NifcFireRaw",
    "OpenMeteoFlood",
    "OpenMeteoFloodEtl",
    "OpenMeteoFloodRaw",
    "SupabaseConnectionConfig",
    "UsgsEarthquake",
    "UsgsEarthquakeEtl",
    "UsgsEarthquakeRaw",
    "UsgsNwis",
    "UsgsNwisRaw",
    "UsgsVolcano",
    "UsgsVolcanoRaw",
]
