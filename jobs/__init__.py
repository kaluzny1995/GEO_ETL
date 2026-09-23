from model.enums import JobType
from factory import JobFactory, UploadScriptFactory, Raw2PlainScriptFactory
from jobs import create_table, ingestion


def _register():
    JobFactory.register(JobType.CREATE, create_table)
    JobFactory.register(JobType.INGESTION, ingestion)

    from upload.avalanche_org_upload import upload as avalanche_org_upload
    from raw2plain.avalanche_org_raw2plain import raw2plain as avalanche_org_raw2plain
    UploadScriptFactory.register("avalanche_org_raw", avalanche_org_upload)
    Raw2PlainScriptFactory.register("avalanche_org_raw", avalanche_org_raw2plain)

    from upload.eonet_event_upload import upload as eonet_event_upload
    from raw2plain.eonet_event_raw2plain import raw2plain as eonet_event_raw2plain
    UploadScriptFactory.register("eonet_event_raw", eonet_event_upload)
    Raw2PlainScriptFactory.register("eonet_event_raw", eonet_event_raw2plain)

    from upload.gdacs_event_upload import upload as gdacs_event_upload
    from raw2plain.gdacs_event_raw2plain import raw2plain as gdacs_event_raw2plain
    UploadScriptFactory.register("gdacs_event_raw", gdacs_event_upload)
    Raw2PlainScriptFactory.register("gdacs_event_raw", gdacs_event_raw2plain)

    from upload.ibtracs_cyclone_upload import upload as ibtracs_cyclone_upload
    from raw2plain.ibtracs_cyclone_raw2plain import raw2plain as ibtracs_cyclone_raw2plain
    UploadScriptFactory.register("ibtracs_cyclone_raw", ibtracs_cyclone_upload)
    Raw2PlainScriptFactory.register("ibtracs_cyclone_raw", ibtracs_cyclone_raw2plain)

    from upload.nifc_fire_upload import upload as nifc_fire_upload
    from raw2plain.nifc_fire_raw2plain import raw2plain as nifc_fire_raw2plain
    UploadScriptFactory.register("nifc_fire_raw", nifc_fire_upload)
    Raw2PlainScriptFactory.register("nifc_fire_raw", nifc_fire_raw2plain)

    from upload.openmeteo_flood_upload import upload as openmeteo_flood_upload
    from raw2plain.openmeteo_flood_raw2plain import raw2plain as openmeteo_flood_raw2plain
    UploadScriptFactory.register("openmeteo_flood_raw", openmeteo_flood_upload)
    Raw2PlainScriptFactory.register("openmeteo_flood_raw", openmeteo_flood_raw2plain)

    from upload.usgs_earthquake_upload import upload as usgs_earthquake_upload
    from raw2plain.usgs_earthquake_raw2plain import raw2plain as usgs_earthquake_raw2plain
    UploadScriptFactory.register("usgs_earthquake_raw", usgs_earthquake_upload)
    Raw2PlainScriptFactory.register("usgs_earthquake_raw", usgs_earthquake_raw2plain)

    from upload.usgs_nwis_upload import upload as usgs_nwis_upload
    from raw2plain.usgs_nwis_raw2plain import raw2plain as usgs_nwis_raw2plain
    UploadScriptFactory.register("usgs_nwis_raw", usgs_nwis_upload)
    Raw2PlainScriptFactory.register("usgs_nwis_raw", usgs_nwis_raw2plain)

    from upload.usgs_volcano_upload import upload as usgs_volcano_upload
    from raw2plain.usgs_volcano_raw2plain import raw2plain as usgs_volcano_raw2plain
    UploadScriptFactory.register("usgs_volcano_raw", usgs_volcano_upload)
    Raw2PlainScriptFactory.register("usgs_volcano_raw", usgs_volcano_raw2plain)


_register()
