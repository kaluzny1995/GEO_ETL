from model.enums import JobType


def test_job_type_create_value():
    assert JobType.CREATE.value == "CREATE"


def test_job_type_ingestion_value():
    assert JobType.INGESTION.value == "INGESTION"


def test_job_type_create_is_instance():
    assert isinstance(JobType.CREATE, JobType)


def test_job_type_ingestion_is_instance():
    assert isinstance(JobType.INGESTION, JobType)


def test_job_type_has_two_members():
    assert len(JobType) == 2


def test_job_type_from_value():
    assert JobType("CREATE") is JobType.CREATE
    assert JobType("INGESTION") is JobType.INGESTION


def test_job_type_invalid_value_raises():
    import pytest

    with pytest.raises(ValueError):
        JobType("INVALID")
