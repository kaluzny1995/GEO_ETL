from unittest.mock import MagicMock, patch

import pytest

from factory import Raw2PlainScriptFactory, UploadScriptFactory


def _mock_app_config():
    cfg = MagicMock()
    cfg.database.db_schema = "geo"
    cfg.database.jdbc_driver = "org.postgresql.Driver"
    cfg.logging.directory = "logs"
    cfg.logging.level_int = 20
    cfg.data.directory = "../GEO_Download/data"
    return cfg


def _mock_db_config():
    cfg = MagicMock()
    cfg.get_jdbc_url.return_value = "jdbc:postgresql://db.example.com:5432/postgres"
    cfg.get_jdbc_properties.return_value = {
        "user": "user",
        "password": "pass",
        "driver": "org.postgresql.Driver",
    }
    return cfg


@pytest.fixture(autouse=True)
def _clean_factories():
    UploadScriptFactory.clear()
    Raw2PlainScriptFactory.clear()
    yield
    UploadScriptFactory.clear()
    Raw2PlainScriptFactory.clear()


@patch("jobs.ingestion.SupabaseConnectionConfig.from_env", return_value=_mock_db_config())
@patch("jobs.ingestion.AppConfig.from_json", return_value=_mock_app_config())
def test_ingestion_runs_upload_then_raw2plain(mock_app, mock_db):
    spark = MagicMock()
    upload_script = MagicMock()
    raw2plain_script = MagicMock()

    UploadScriptFactory.register("avalanche_org_raw", upload_script)
    Raw2PlainScriptFactory.register("avalanche_org_raw", raw2plain_script)

    from jobs.ingestion import run

    result = run(spark, "avalanche_org_raw")

    assert upload_script.upload.call_count == 1
    assert raw2plain_script.raw2plain.call_count == 1
    assert upload_script.upload.call_args[0][0] is spark
    assert raw2plain_script.raw2plain.call_args[0][0] is spark
    assert "complete" in result


@patch("jobs.ingestion.SupabaseConnectionConfig.from_env", return_value=_mock_db_config())
@patch("jobs.ingestion.AppConfig.from_json", return_value=_mock_app_config())
def test_ingestion_upload_before_raw2plain(mock_app, mock_db):
    spark = MagicMock()
    call_order = []

    def _record(name):
        """Fake upload/raw2plain step that records its name and reports work done."""

        def _step(*args, **kwargs):
            call_order.append(name)
            return True

        return _step

    upload_script = MagicMock()
    upload_script.upload.side_effect = _record("upload")
    raw2plain_script = MagicMock()
    raw2plain_script.raw2plain.side_effect = _record("raw2plain")

    UploadScriptFactory.register("t", upload_script)
    Raw2PlainScriptFactory.register("t", raw2plain_script)

    from jobs.ingestion import run

    run(spark, "t")

    assert call_order == ["upload", "raw2plain"]


@patch("jobs.ingestion.SupabaseConnectionConfig.from_env", return_value=_mock_db_config())
@patch("jobs.ingestion.AppConfig.from_json", return_value=_mock_app_config())
def test_ingestion_passes_config_and_db_config(mock_app, mock_db):
    spark = MagicMock()
    upload_script = MagicMock()
    raw2plain_script = MagicMock()

    UploadScriptFactory.register("t", upload_script)
    Raw2PlainScriptFactory.register("t", raw2plain_script)

    from jobs.ingestion import run

    run(spark, "t")

    app_cfg = upload_script.upload.call_args[0][1]
    db_cfg = upload_script.upload.call_args[0][2]
    assert app_cfg is mock_app.return_value
    assert db_cfg is mock_db.return_value


@patch("jobs.ingestion.SupabaseConnectionConfig.from_env", return_value=_mock_db_config())
@patch("jobs.ingestion.AppConfig.from_json", return_value=_mock_app_config())
def test_ingestion_passes_logger_to_upload_and_raw2plain(mock_app, mock_db):
    spark = MagicMock()
    upload_script = MagicMock()
    raw2plain_script = MagicMock()

    UploadScriptFactory.register("t", upload_script)
    Raw2PlainScriptFactory.register("t", raw2plain_script)

    log = MagicMock()
    from jobs.ingestion import run

    run(spark, "t", logger=log)

    assert upload_script.upload.call_args.kwargs["logger"] is log
    assert raw2plain_script.raw2plain.call_args.kwargs["logger"] is log


@patch("jobs.ingestion.SupabaseConnectionConfig.from_env", return_value=_mock_db_config())
@patch("jobs.ingestion.AppConfig.from_json", return_value=_mock_app_config())
def test_ingestion_logs_steps(mock_app, mock_db):
    spark = MagicMock()
    UploadScriptFactory.register("t", MagicMock())
    Raw2PlainScriptFactory.register("t", MagicMock())

    log = MagicMock()
    from jobs.ingestion import run

    run(spark, "t", logger=log)

    log.info.assert_any_call("Running upload for t...")
    log.info.assert_any_call("Upload for t complete")
    log.info.assert_any_call("Running raw2plain for t...")
    log.info.assert_any_call("Raw2Plain for t complete")


@patch("jobs.ingestion.SupabaseConnectionConfig.from_env", return_value=_mock_db_config())
@patch("jobs.ingestion.AppConfig.from_json", return_value=_mock_app_config())
def test_ingestion_fails_when_nothing_to_ingest(mock_app, mock_db):
    """No new file and no pending raw rows → RuntimeError, so the E2E chain stops."""
    # Arrange
    spark = MagicMock()
    upload_script = MagicMock()
    upload_script.upload.return_value = False
    UploadScriptFactory.register("t", upload_script)
    raw2plain_script = MagicMock()
    raw2plain_script.raw2plain.return_value = False
    Raw2PlainScriptFactory.register("t", raw2plain_script)

    from jobs.ingestion import run

    # Act & Assert
    with pytest.raises(RuntimeError, match="No new data to ingest for t"):
        run(spark, "t")


@patch("jobs.ingestion.SupabaseConnectionConfig.from_env", return_value=_mock_db_config())
@patch("jobs.ingestion.AppConfig.from_json", return_value=_mock_app_config())
def test_ingestion_succeeds_when_only_upload_found_new_files(mock_app, mock_db):
    """A new CSV that produced no new plain rows is still a successful run."""
    # Arrange
    spark = MagicMock()
    upload_script = MagicMock()
    upload_script.upload.return_value = True
    UploadScriptFactory.register("t", upload_script)
    raw2plain_script = MagicMock()
    raw2plain_script.raw2plain.return_value = False
    Raw2PlainScriptFactory.register("t", raw2plain_script)

    from jobs.ingestion import run

    # Act
    result = run(spark, "t")

    # Assert
    assert "complete" in result


@patch("jobs.ingestion.SupabaseConnectionConfig.from_env", return_value=_mock_db_config())
@patch("jobs.ingestion.AppConfig.from_json", return_value=_mock_app_config())
def test_ingestion_succeeds_when_only_raw2plain_has_pending_rows(mock_app, mock_db):
    """Raw rows left by an earlier crash are transformed — the run must not fail."""
    # Arrange
    spark = MagicMock()
    upload_script = MagicMock()
    upload_script.upload.return_value = False
    UploadScriptFactory.register("t", upload_script)
    raw2plain_script = MagicMock()
    raw2plain_script.raw2plain.return_value = True
    Raw2PlainScriptFactory.register("t", raw2plain_script)

    from jobs.ingestion import run

    # Act
    result = run(spark, "t")

    # Assert
    assert "complete" in result
