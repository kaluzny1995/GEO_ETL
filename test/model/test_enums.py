from model.enums import JobType, LoadType


def test_job_type_create_value():
    assert JobType.CREATE.value == "CREATE"


def test_job_type_ingestion_value():
    assert JobType.INGESTION.value == "INGESTION"


def test_job_type_data_quality_value():
    assert JobType.DATA_QUALITY.value == "DATA_QUALITY"


def test_job_type_single_etl_value():
    assert JobType.SINGLE_ETL.value == "SINGLE_ETL"


def test_job_type_create_is_instance():
    assert isinstance(JobType.CREATE, JobType)


def test_job_type_ingestion_is_instance():
    assert isinstance(JobType.INGESTION, JobType)


def test_job_type_data_quality_is_instance():
    assert isinstance(JobType.DATA_QUALITY, JobType)


def test_job_type_single_etl_is_instance():
    assert isinstance(JobType.SINGLE_ETL, JobType)


def test_job_type_has_four_members():
    assert len(JobType) == 4


def test_job_type_from_value():
    assert JobType("CREATE") is JobType.CREATE
    assert JobType("INGESTION") is JobType.INGESTION
    assert JobType("DATA_QUALITY") is JobType.DATA_QUALITY
    assert JobType("SINGLE_ETL") is JobType.SINGLE_ETL


def test_job_type_invalid_value_raises():
    import pytest

    with pytest.raises(ValueError):
        JobType("INVALID")


def test_load_type_values():
    assert LoadType.HISTORICAL.value == "HISTORICAL"
    assert LoadType.INCREMENTAL.value == "INCREMENTAL"


def test_load_type_has_two_members():
    assert len(LoadType) == 2


def test_load_type_from_value():
    assert LoadType("HISTORICAL") is LoadType.HISTORICAL
    assert LoadType("INCREMENTAL") is LoadType.INCREMENTAL


def test_load_type_invalid_value_raises():
    import pytest

    with pytest.raises(ValueError):
        LoadType("AUTO")
