from factory import JobFactory, Raw2PlainScriptFactory, UploadScriptFactory
from jobs import create_table, data_quality, ingestion
from model.enums import JobType


def _register():
    JobFactory.register(JobType.CREATE, create_table)
    JobFactory.register(JobType.INGESTION, ingestion)
    JobFactory.register(JobType.DATA_QUALITY, data_quality)

    from ingestion.raw2plain.avalanche_org_raw2plain import raw2plain as avalanche_org_raw2plain
    from ingestion.upload.avalanche_org_upload import upload as avalanche_org_upload

    UploadScriptFactory.register("avalanche_org_raw", avalanche_org_upload)
    Raw2PlainScriptFactory.register("avalanche_org_raw", avalanche_org_raw2plain)

    from ingestion.raw2plain.eonet_event_raw2plain import raw2plain as eonet_event_raw2plain
    from ingestion.upload.eonet_event_upload import upload as eonet_event_upload

    UploadScriptFactory.register("eonet_event_raw", eonet_event_upload)
    Raw2PlainScriptFactory.register("eonet_event_raw", eonet_event_raw2plain)

    from ingestion.raw2plain.gdacs_event_raw2plain import raw2plain as gdacs_event_raw2plain
    from ingestion.upload.gdacs_event_upload import upload as gdacs_event_upload

    UploadScriptFactory.register("gdacs_event_raw", gdacs_event_upload)
    Raw2PlainScriptFactory.register("gdacs_event_raw", gdacs_event_raw2plain)

    from ingestion.raw2plain.ibtracs_cyclone_raw2plain import raw2plain as ibtracs_cyclone_raw2plain
    from ingestion.upload.ibtracs_cyclone_upload import upload as ibtracs_cyclone_upload

    UploadScriptFactory.register("ibtracs_cyclone_raw", ibtracs_cyclone_upload)
    Raw2PlainScriptFactory.register("ibtracs_cyclone_raw", ibtracs_cyclone_raw2plain)

    from ingestion.raw2plain.nifc_fire_raw2plain import raw2plain as nifc_fire_raw2plain
    from ingestion.upload.nifc_fire_upload import upload as nifc_fire_upload

    UploadScriptFactory.register("nifc_fire_raw", nifc_fire_upload)
    Raw2PlainScriptFactory.register("nifc_fire_raw", nifc_fire_raw2plain)

    from ingestion.raw2plain.openmeteo_flood_raw2plain import raw2plain as openmeteo_flood_raw2plain
    from ingestion.upload.openmeteo_flood_upload import upload as openmeteo_flood_upload

    UploadScriptFactory.register("openmeteo_flood_raw", openmeteo_flood_upload)
    Raw2PlainScriptFactory.register("openmeteo_flood_raw", openmeteo_flood_raw2plain)

    from ingestion.raw2plain.usgs_earthquake_raw2plain import raw2plain as usgs_earthquake_raw2plain
    from ingestion.upload.usgs_earthquake_upload import upload as usgs_earthquake_upload

    UploadScriptFactory.register("usgs_earthquake_raw", usgs_earthquake_upload)
    Raw2PlainScriptFactory.register("usgs_earthquake_raw", usgs_earthquake_raw2plain)

    from ingestion.raw2plain.usgs_nwis_raw2plain import raw2plain as usgs_nwis_raw2plain
    from ingestion.upload.usgs_nwis_upload import upload as usgs_nwis_upload

    UploadScriptFactory.register("usgs_nwis_raw", usgs_nwis_upload)
    Raw2PlainScriptFactory.register("usgs_nwis_raw", usgs_nwis_raw2plain)

    from ingestion.raw2plain.usgs_volcano_raw2plain import raw2plain as usgs_volcano_raw2plain
    from ingestion.upload.usgs_volcano_upload import upload as usgs_volcano_upload

    UploadScriptFactory.register("usgs_volcano_raw", usgs_volcano_upload)
    Raw2PlainScriptFactory.register("usgs_volcano_raw", usgs_volcano_raw2plain)


_register()
