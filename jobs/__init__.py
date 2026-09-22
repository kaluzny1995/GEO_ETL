from model.enums import JobType
from factory import JobFactory, UploadScriptFactory, Raw2FinalScriptFactory
from jobs import create_table, ingestion
from upload.avalanche_org_upload import upload
from raw2final.avalanche_org_raw2final import raw2final

JobFactory.register(JobType.CREATE, create_table)
JobFactory.register(JobType.INGESTION, ingestion)

UploadScriptFactory.register("avalanche_org_raw", upload)
Raw2FinalScriptFactory.register("avalanche_org_raw", raw2final)
