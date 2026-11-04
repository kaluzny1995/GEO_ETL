from model.enums import DangerRating, JobType, LoadType


def test_job_type_create_value():
    assert JobType.CREATE.value == "CREATE"


def test_job_type_ingestion_value():
    assert JobType.INGESTION.value == "INGESTION"


def test_job_type_data_quality_value():
    assert JobType.DATA_QUALITY.value == "DATA_QUALITY"


def test_job_type_single_etl_value():
    assert JobType.SINGLE_ETL.value == "SINGLE_ETL"


def test_job_type_view_value():
    assert JobType.VIEW.value == "VIEW"


def test_job_type_create_is_instance():
    assert isinstance(JobType.CREATE, JobType)


def test_job_type_ingestion_is_instance():
    assert isinstance(JobType.INGESTION, JobType)


def test_job_type_data_quality_is_instance():
    assert isinstance(JobType.DATA_QUALITY, JobType)


def test_job_type_data_source_file_is_instance():
    assert isinstance(JobType.DATA_SOURCE_FILE, JobType)


def test_job_type_single_etl_is_instance():
    assert isinstance(JobType.SINGLE_ETL, JobType)


def test_job_type_view_is_instance():
    assert isinstance(JobType.VIEW, JobType)


def test_job_type_has_six_members():
    assert len(JobType) == 6


def test_job_type_from_value():
    assert JobType("CREATE") is JobType.CREATE
    assert JobType("INGESTION") is JobType.INGESTION
    assert JobType("DATA_QUALITY") is JobType.DATA_QUALITY
    assert JobType("DATA_SOURCE_FILE") is JobType.DATA_SOURCE_FILE
    assert JobType("SINGLE_ETL") is JobType.SINGLE_ETL
    assert JobType("VIEW") is JobType.VIEW


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


def test_danger_rating_values():
    assert DangerRating.NO_RATING.value == "no_rating"
    assert DangerRating.LOW.value == "low"
    assert DangerRating.MEDIUM.value == "medium"
    assert DangerRating.HIGH.value == "high"
    assert DangerRating.EXTREME.value == "extreme"


def test_danger_rating_has_five_members_in_severity_order():
    assert len(DangerRating) == 5
    assert list(DangerRating) == [
        DangerRating.NO_RATING,
        DangerRating.LOW,
        DangerRating.MEDIUM,
        DangerRating.HIGH,
        DangerRating.EXTREME,
    ]


def test_danger_rating_is_a_string_enum():
    assert isinstance(DangerRating.HIGH, str)
    assert DangerRating.HIGH == "high"
    assert f"{DangerRating.HIGH}" == "high"


def test_danger_rating_from_value():
    assert DangerRating("high") is DangerRating.HIGH
    assert DangerRating("no_rating") is DangerRating.NO_RATING


def test_danger_rating_invalid_value_raises():
    import pytest

    with pytest.raises(ValueError):
        DangerRating("catastrophic")
