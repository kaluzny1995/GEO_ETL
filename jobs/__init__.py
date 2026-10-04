from factory import JobFactory, Raw2PlainScriptFactory, SingleEtlFactory, UploadScriptFactory
from jobs import create_table, data_quality, ingestion, single_etl
from model.enums import JobType


def _register():
    JobFactory.register(JobType.CREATE, create_table)
    JobFactory.register(JobType.INGESTION, ingestion)
    JobFactory.register(JobType.DATA_QUALITY, data_quality)
    JobFactory.register(JobType.SINGLE_ETL, single_etl)

    from single_etl.avalanche_org_etl import AvalancheOrgEtlScript

    SingleEtlFactory.register("avalanche_org_etl", AvalancheOrgEtlScript())

    from single_etl.eonet_event_etl import EonetEventEtlScript

    SingleEtlFactory.register("eonet_event_etl", EonetEventEtlScript())

    from single_etl.gdacs_event_etl import GdacsEventEtlScript

    SingleEtlFactory.register("gdacs_event_etl", GdacsEventEtlScript())

    from single_etl.ibtracs_cyclone_etl import IbtracsCycloneEtlScript

    SingleEtlFactory.register("ibtracs_cyclone_etl", IbtracsCycloneEtlScript())

    from single_etl.nifc_fire_etl import NifcFireEtlScript

    SingleEtlFactory.register("nifc_fire_etl", NifcFireEtlScript())

    from single_etl.openmeteo_flood_etl import OpenMeteoFloodEtlScript

    SingleEtlFactory.register("openmeteo_flood_etl", OpenMeteoFloodEtlScript())

    from single_etl.usgs_earthquake_etl import UsgsEarthquakeEtlScript

    SingleEtlFactory.register("usgs_earthquake_etl", UsgsEarthquakeEtlScript())

    from single_etl.usgs_nwis_etl import UsgsNwisEtlScript

    SingleEtlFactory.register("usgs_nwis_etl", UsgsNwisEtlScript())

    from single_etl.usgs_volcano_etl import UsgsVolcanoEtlScript

    SingleEtlFactory.register("usgs_volcano_etl", UsgsVolcanoEtlScript())

    from ingestion.raw2plain.avalanche_org_raw2plain import AvalancheOrgRaw2PlainScript
    from ingestion.upload.avalanche_org_upload import AvalancheOrgUploadScript

    UploadScriptFactory.register("avalanche_org_raw", AvalancheOrgUploadScript())
    Raw2PlainScriptFactory.register("avalanche_org_raw", AvalancheOrgRaw2PlainScript())

    from ingestion.raw2plain.eonet_event_raw2plain import EonetEventRaw2PlainScript
    from ingestion.upload.eonet_event_upload import EonetEventUploadScript

    UploadScriptFactory.register("eonet_event_raw", EonetEventUploadScript())
    Raw2PlainScriptFactory.register("eonet_event_raw", EonetEventRaw2PlainScript())

    from ingestion.raw2plain.gdacs_event_raw2plain import GdacsEventRaw2PlainScript
    from ingestion.upload.gdacs_event_upload import GdacsEventUploadScript

    UploadScriptFactory.register("gdacs_event_raw", GdacsEventUploadScript())
    Raw2PlainScriptFactory.register("gdacs_event_raw", GdacsEventRaw2PlainScript())

    from ingestion.raw2plain.ibtracs_cyclone_raw2plain import IbtracsCycloneRaw2PlainScript
    from ingestion.upload.ibtracs_cyclone_upload import IbtracsCycloneUploadScript

    UploadScriptFactory.register("ibtracs_cyclone_raw", IbtracsCycloneUploadScript())
    Raw2PlainScriptFactory.register("ibtracs_cyclone_raw", IbtracsCycloneRaw2PlainScript())

    from ingestion.raw2plain.nifc_fire_raw2plain import NifcFireRaw2PlainScript
    from ingestion.upload.nifc_fire_upload import NifcFireUploadScript

    UploadScriptFactory.register("nifc_fire_raw", NifcFireUploadScript())
    Raw2PlainScriptFactory.register("nifc_fire_raw", NifcFireRaw2PlainScript())

    from ingestion.raw2plain.openmeteo_flood_raw2plain import OpenMeteoFloodRaw2PlainScript
    from ingestion.upload.openmeteo_flood_upload import OpenMeteoFloodUploadScript

    UploadScriptFactory.register("openmeteo_flood_raw", OpenMeteoFloodUploadScript())
    Raw2PlainScriptFactory.register("openmeteo_flood_raw", OpenMeteoFloodRaw2PlainScript())

    from ingestion.raw2plain.usgs_earthquake_raw2plain import UsgsEarthquakeRaw2PlainScript
    from ingestion.upload.usgs_earthquake_upload import UsgsEarthquakeUploadScript

    UploadScriptFactory.register("usgs_earthquake_raw", UsgsEarthquakeUploadScript())
    Raw2PlainScriptFactory.register("usgs_earthquake_raw", UsgsEarthquakeRaw2PlainScript())

    from ingestion.raw2plain.usgs_nwis_raw2plain import UsgsNwisRaw2PlainScript
    from ingestion.upload.usgs_nwis_upload import UsgsNwisUploadScript

    UploadScriptFactory.register("usgs_nwis_raw", UsgsNwisUploadScript())
    Raw2PlainScriptFactory.register("usgs_nwis_raw", UsgsNwisRaw2PlainScript())

    from ingestion.raw2plain.usgs_volcano_raw2plain import UsgsVolcanoRaw2PlainScript
    from ingestion.upload.usgs_volcano_upload import UsgsVolcanoUploadScript

    UploadScriptFactory.register("usgs_volcano_raw", UsgsVolcanoUploadScript())
    Raw2PlainScriptFactory.register("usgs_volcano_raw", UsgsVolcanoRaw2PlainScript())


_register()
