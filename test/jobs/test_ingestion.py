from unittest.mock import MagicMock, patch

import pytest

from factory import UploadScriptFactory
from test.conftest import mock_app_config, mock_db_config


@pytest.fixture(autouse=True)
def _clean_factory():
    UploadScriptFactory.clear()
    yield
    UploadScriptFactory.clear()


@patch("jobs.ingestion.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch(
    "jobs.ingestion.AppConfig.from_json",
    return_value=mock_app_config(logging=True, data_directory="../GEO_Download/data"),
)
def test_ingestion_runs_upload(mock_app, mock_db):
    spark = MagicMock()
    upload_script = MagicMock()
    upload_script.upload.return_value = True

    UploadScriptFactory.register("avalanche_org", upload_script)

    from jobs.ingestion import run

    result = run(spark, "avalanche_org")

    assert upload_script.upload.call_count == 1
    assert upload_script.upload.call_args[0][0] is spark
    assert "complete" in result


@patch("jobs.ingestion.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch(
    "jobs.ingestion.AppConfig.from_json",
    return_value=mock_app_config(logging=True, data_directory="../GEO_Download/data"),
)
def test_ingestion_passes_config_and_db_config(mock_app, mock_db):
    spark = MagicMock()
    upload_script = MagicMock()
    upload_script.upload.return_value = True

    UploadScriptFactory.register("avalanche_org", upload_script)

    from jobs.ingestion import run

    run(spark, "avalanche_org")

    app_cfg = upload_script.upload.call_args[0][1]
    db_cfg = upload_script.upload.call_args[0][2]
    assert app_cfg is mock_app.return_value
    assert db_cfg is mock_db.return_value


@patch("jobs.ingestion.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch(
    "jobs.ingestion.AppConfig.from_json",
    return_value=mock_app_config(logging=True, data_directory="../GEO_Download/data"),
)
def test_ingestion_passes_logger_to_upload(mock_app, mock_db):
    spark = MagicMock()
    upload_script = MagicMock()
    upload_script.upload.return_value = True

    UploadScriptFactory.register("avalanche_org", upload_script)

    log = MagicMock()
    from jobs.ingestion import run

    run(spark, "avalanche_org", logger=log)

    assert upload_script.upload.call_args.kwargs["logger"] is log


@patch("jobs.ingestion.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch(
    "jobs.ingestion.AppConfig.from_json",
    return_value=mock_app_config(logging=True, data_directory="../GEO_Download/data"),
)
def test_ingestion_logs_steps(mock_app, mock_db):
    spark = MagicMock()
    upload_script = MagicMock()
    upload_script.upload.return_value = True
    UploadScriptFactory.register("avalanche_org", upload_script)

    log = MagicMock()
    from jobs.ingestion import run

    run(spark, "avalanche_org", logger=log)

    log.info.assert_any_call("Running upload for avalanche_org...")
    log.info.assert_any_call("Upload for avalanche_org complete")


@patch("jobs.ingestion.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch(
    "jobs.ingestion.AppConfig.from_json",
    return_value=mock_app_config(logging=True, data_directory="../GEO_Download/data"),
)
def test_ingestion_fails_when_nothing_to_ingest(mock_app, mock_db):
    """No CSV newer than the plain table's latest file_date → RuntimeError."""
    # Arrange
    spark = MagicMock()
    upload_script = MagicMock()
    upload_script.upload.return_value = False
    UploadScriptFactory.register("avalanche_org", upload_script)

    from jobs.ingestion import run

    # Act & Assert
    with pytest.raises(RuntimeError, match="No new data to ingest for avalanche_org"):
        run(spark, "avalanche_org")
