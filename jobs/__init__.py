from model.enums import JobType
from factory import JobFactory, UploadScriptFactory, Raw2PlainScriptFactory
from jobs import create_table, ingestion


def _register():
    JobFactory.register(JobType.CREATE, create_table)
    JobFactory.register(JobType.INGESTION, ingestion)

    from upload.avalanche_org_upload import upload
    from raw2plain.avalanche_org_raw2plain import raw2plain

    UploadScriptFactory.register("avalanche_org_raw", upload)
    Raw2PlainScriptFactory.register("avalanche_org_raw", raw2plain)


_register()
