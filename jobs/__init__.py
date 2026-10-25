from factory import JobFactory, Raw2PlainScriptFactory, SingleEtlFactory, UploadScriptFactory, ViewScriptFactory
from jobs import create_table, data_quality, ingestion, single_etl, view
from model.enums import JobType


def _register():
    JobFactory.register(JobType.CREATE, create_table)
    JobFactory.register(JobType.INGESTION, ingestion)
    JobFactory.register(JobType.DATA_QUALITY, data_quality)
    JobFactory.register(JobType.SINGLE_ETL, single_etl)
    JobFactory.register(JobType.VIEW, view)

    from single_etl.avalanche_org_etl import AvalancheOrgEtlScript

    _script = AvalancheOrgEtlScript()
    SingleEtlFactory.register(_script.TARGET_TABLE, _script)

    from single_etl.eonet_event_etl import EonetEventEtlScript

    _script = EonetEventEtlScript()
    SingleEtlFactory.register(_script.TARGET_TABLE, _script)

    from single_etl.gdacs_event_etl import GdacsEventEtlScript

    _script = GdacsEventEtlScript()
    SingleEtlFactory.register(_script.TARGET_TABLE, _script)

    from single_etl.ibtracs_cyclone_etl import IbtracsCycloneEtlScript

    _script = IbtracsCycloneEtlScript()
    SingleEtlFactory.register(_script.TARGET_TABLE, _script)

    from single_etl.nifc_fire_etl import NifcFireEtlScript

    _script = NifcFireEtlScript()
    SingleEtlFactory.register(_script.TARGET_TABLE, _script)

    from single_etl.openmeteo_flood_etl import OpenMeteoFloodEtlScript

    _script = OpenMeteoFloodEtlScript()
    SingleEtlFactory.register(_script.TARGET_TABLE, _script)

    from single_etl.usgs_earthquake_etl import UsgsEarthquakeEtlScript

    _script = UsgsEarthquakeEtlScript()
    SingleEtlFactory.register(_script.TARGET_TABLE, _script)

    from single_etl.usgs_nwis_etl import UsgsNwisEtlScript

    _script = UsgsNwisEtlScript()
    SingleEtlFactory.register(_script.TARGET_TABLE, _script)

    from single_etl.usgs_volcano_etl import UsgsVolcanoEtlScript

    _script = UsgsVolcanoEtlScript()
    SingleEtlFactory.register(_script.TARGET_TABLE, _script)

    from single_etl.union_etl import UnionEtlScript

    _script = UnionEtlScript()
    SingleEtlFactory.register(_script.TARGET_TABLE, _script)

    from views.metrics_avalanche_vw import MetricsAvalancheVwScript

    _script = MetricsAvalancheVwScript()
    ViewScriptFactory.register(_script.TARGET_VIEW, _script)

    from views.metrics_drought_vw import MetricsDroughtVwScript

    _script = MetricsDroughtVwScript()
    ViewScriptFactory.register(_script.TARGET_VIEW, _script)

    from views.metrics_earthquake_vw import MetricsEarthquakeVwScript

    _script = MetricsEarthquakeVwScript()
    ViewScriptFactory.register(_script.TARGET_VIEW, _script)

    from views.metrics_fire_vw import MetricsFireVwScript

    _script = MetricsFireVwScript()
    ViewScriptFactory.register(_script.TARGET_VIEW, _script)

    from views.metrics_flood_vw import MetricsFloodVwScript

    _script = MetricsFloodVwScript()
    ViewScriptFactory.register(_script.TARGET_VIEW, _script)

    from views.metrics_iceberg_vw import MetricsIcebergVwScript

    _script = MetricsIcebergVwScript()
    ViewScriptFactory.register(_script.TARGET_VIEW, _script)

    from views.metrics_storm_vw import MetricsStormVwScript

    _script = MetricsStormVwScript()
    ViewScriptFactory.register(_script.TARGET_VIEW, _script)

    from views.metrics_volcano_vw import MetricsVolcanoVwScript

    _script = MetricsVolcanoVwScript()
    ViewScriptFactory.register(_script.TARGET_VIEW, _script)

    from views.phenomenons_us_vw import PhenomenonsUsVwScript

    _script = PhenomenonsUsVwScript()
    ViewScriptFactory.register(_script.TARGET_VIEW, _script)

    from views.phenomenons_ca_vw import PhenomenonsCaVwScript

    _script = PhenomenonsCaVwScript()
    ViewScriptFactory.register(_script.TARGET_VIEW, _script)

    from views.phenomenons_cs_amer_vw import PhenomenonsCsAmerVwScript

    _script = PhenomenonsCsAmerVwScript()
    ViewScriptFactory.register(_script.TARGET_VIEW, _script)

    from views.phenomenons_ru_vw import PhenomenonsRuVwScript

    _script = PhenomenonsRuVwScript()
    ViewScriptFactory.register(_script.TARGET_VIEW, _script)

    from views.phenomenons_europe_vw import PhenomenonsEuropeVwScript

    _script = PhenomenonsEuropeVwScript()
    ViewScriptFactory.register(_script.TARGET_VIEW, _script)

    from views.phenomenons_asia_vw import PhenomenonsAsiaVwScript

    _script = PhenomenonsAsiaVwScript()
    ViewScriptFactory.register(_script.TARGET_VIEW, _script)

    from views.phenomenons_africa_vw import PhenomenonsAfricaVwScript

    _script = PhenomenonsAfricaVwScript()
    ViewScriptFactory.register(_script.TARGET_VIEW, _script)

    from views.phenomenons_aus_oc_vw import PhenomenonsAusOcVwScript

    _script = PhenomenonsAusOcVwScript()
    ViewScriptFactory.register(_script.TARGET_VIEW, _script)

    from ingestion.raw2plain.avalanche_org_raw2plain import AvalancheOrgRaw2PlainScript
    from ingestion.upload.avalanche_org_upload import AvalancheOrgUploadScript

    _upload = AvalancheOrgUploadScript()
    UploadScriptFactory.register(_upload.TABLE_NAME, _upload)
    _r2p = AvalancheOrgRaw2PlainScript()
    Raw2PlainScriptFactory.register(_r2p.RAW_TABLE, _r2p)

    from ingestion.raw2plain.eonet_event_raw2plain import EonetEventRaw2PlainScript
    from ingestion.upload.eonet_event_upload import EonetEventUploadScript

    _upload = EonetEventUploadScript()
    UploadScriptFactory.register(_upload.TABLE_NAME, _upload)
    _r2p = EonetEventRaw2PlainScript()
    Raw2PlainScriptFactory.register(_r2p.RAW_TABLE, _r2p)

    from ingestion.raw2plain.gdacs_event_raw2plain import GdacsEventRaw2PlainScript
    from ingestion.upload.gdacs_event_upload import GdacsEventUploadScript

    _upload = GdacsEventUploadScript()
    UploadScriptFactory.register(_upload.TABLE_NAME, _upload)
    _r2p = GdacsEventRaw2PlainScript()
    Raw2PlainScriptFactory.register(_r2p.RAW_TABLE, _r2p)

    from ingestion.raw2plain.ibtracs_cyclone_raw2plain import IbtracsCycloneRaw2PlainScript
    from ingestion.upload.ibtracs_cyclone_upload import IbtracsCycloneUploadScript

    _upload = IbtracsCycloneUploadScript()
    UploadScriptFactory.register(_upload.TABLE_NAME, _upload)
    _r2p = IbtracsCycloneRaw2PlainScript()
    Raw2PlainScriptFactory.register(_r2p.RAW_TABLE, _r2p)

    from ingestion.raw2plain.nifc_fire_raw2plain import NifcFireRaw2PlainScript
    from ingestion.upload.nifc_fire_upload import NifcFireUploadScript

    _upload = NifcFireUploadScript()
    UploadScriptFactory.register(_upload.TABLE_NAME, _upload)
    _r2p = NifcFireRaw2PlainScript()
    Raw2PlainScriptFactory.register(_r2p.RAW_TABLE, _r2p)

    from ingestion.raw2plain.openmeteo_flood_raw2plain import OpenMeteoFloodRaw2PlainScript
    from ingestion.upload.openmeteo_flood_upload import OpenMeteoFloodUploadScript

    _upload = OpenMeteoFloodUploadScript()
    UploadScriptFactory.register(_upload.TABLE_NAME, _upload)
    _r2p = OpenMeteoFloodRaw2PlainScript()
    Raw2PlainScriptFactory.register(_r2p.RAW_TABLE, _r2p)

    from ingestion.raw2plain.usgs_earthquake_raw2plain import UsgsEarthquakeRaw2PlainScript
    from ingestion.upload.usgs_earthquake_upload import UsgsEarthquakeUploadScript

    _upload = UsgsEarthquakeUploadScript()
    UploadScriptFactory.register(_upload.TABLE_NAME, _upload)
    _r2p = UsgsEarthquakeRaw2PlainScript()
    Raw2PlainScriptFactory.register(_r2p.RAW_TABLE, _r2p)

    from ingestion.raw2plain.usgs_nwis_raw2plain import UsgsNwisRaw2PlainScript
    from ingestion.upload.usgs_nwis_upload import UsgsNwisUploadScript

    _upload = UsgsNwisUploadScript()
    UploadScriptFactory.register(_upload.TABLE_NAME, _upload)
    _r2p = UsgsNwisRaw2PlainScript()
    Raw2PlainScriptFactory.register(_r2p.RAW_TABLE, _r2p)

    from ingestion.raw2plain.usgs_volcano_raw2plain import UsgsVolcanoRaw2PlainScript
    from ingestion.upload.usgs_volcano_upload import UsgsVolcanoUploadScript

    _upload = UsgsVolcanoUploadScript()
    UploadScriptFactory.register(_upload.TABLE_NAME, _upload)
    _r2p = UsgsVolcanoRaw2PlainScript()
    Raw2PlainScriptFactory.register(_r2p.RAW_TABLE, _r2p)


_register()
