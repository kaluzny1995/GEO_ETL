from model.config import AppConfig, SupabaseConnectionConfig
from model.enums import JobType
from model.etl.avalanche_org_etl import AvalancheOrgEtl
from model.etl.eonet_event_etl import EonetEventEtl
from model.etl.gdacs_event_etl import GdacsEventEtl
from model.etl.ibtracs_cyclone_etl import IbtracsCycloneEtl
from model.etl.nifc_fire_etl import NifcFireEtl
from model.etl.openmeteo_flood_etl import OpenMeteoFloodEtl
from model.etl.union_etl import UnionEtl
from model.etl.usgs_earthquake_etl import UsgsEarthquakeEtl
from model.etl.usgs_nwis_etl import UsgsNwisEtl
from model.etl.usgs_volcano_etl import UsgsVolcanoEtl
from model.other.data_quality import DataQuality
from model.other.data_source_file import DataSourceFile, DataSourceSource
from model.other.prediction_etl import PredictionEtl
from model.plain.avalanche_org import AvalancheOrg
from model.plain.eonet_event import EonetEvent
from model.plain.gdacs_event import GdacsEvent
from model.plain.ibtracs_cyclone import IbtacsCyclone
from model.plain.nifc_fire import NifcFire
from model.plain.openmeteo_flood import OpenMeteoFlood
from model.plain.usgs_earthquake import UsgsEarthquake
from model.plain.usgs_nwis import UsgsNwis
from model.plain.usgs_volcano import UsgsVolcano


def _register_tables():
    from factory import TableFactory

    TableFactory.register("avalanche_org", AvalancheOrg)
    TableFactory.register("avalanche_org_etl", AvalancheOrgEtl)
    TableFactory.register("eonet_event", EonetEvent)
    TableFactory.register("eonet_event_etl", EonetEventEtl)
    TableFactory.register("gdacs_event", GdacsEvent)
    TableFactory.register("gdacs_event_etl", GdacsEventEtl)
    TableFactory.register("ibtracs_cyclone", IbtacsCyclone)
    TableFactory.register("ibtracs_cyclone_etl", IbtracsCycloneEtl)
    TableFactory.register("nifc_fire", NifcFire)
    TableFactory.register("nifc_fire_etl", NifcFireEtl)
    TableFactory.register("openmeteo_flood", OpenMeteoFlood)
    TableFactory.register("openmeteo_flood_etl", OpenMeteoFloodEtl)
    TableFactory.register("usgs_earthquake", UsgsEarthquake)
    TableFactory.register("usgs_earthquake_etl", UsgsEarthquakeEtl)
    TableFactory.register("usgs_nwis", UsgsNwis)
    TableFactory.register("usgs_nwis_etl", UsgsNwisEtl)
    TableFactory.register("usgs_volcano", UsgsVolcano)
    TableFactory.register("usgs_volcano_etl", UsgsVolcanoEtl)
    TableFactory.register("union_etl", UnionEtl)
    TableFactory.register("data_quality", DataQuality)
    TableFactory.register("prediction_etl", PredictionEtl)


_register_tables()

__all__ = [
    "AppConfig",
    "AvalancheOrg",
    "AvalancheOrgEtl",
    "DataQuality",
    "DataSourceFile",
    "DataSourceSource",
    "EonetEvent",
    "EonetEventEtl",
    "GdacsEvent",
    "GdacsEventEtl",
    "IbtacsCyclone",
    "IbtracsCycloneEtl",
    "JobType",
    "NifcFire",
    "NifcFireEtl",
    "OpenMeteoFlood",
    "OpenMeteoFloodEtl",
    "PredictionEtl",
    "SupabaseConnectionConfig",
    "UnionEtl",
    "UsgsEarthquake",
    "UsgsEarthquakeEtl",
    "UsgsNwis",
    "UsgsNwisEtl",
    "UsgsVolcano",
    "UsgsVolcanoEtl",
]
