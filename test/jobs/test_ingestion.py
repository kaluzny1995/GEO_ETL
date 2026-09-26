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


@patch("jobs.ingestion.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
@patch("jobs.ingestion.AppConfig.from_json", return_value=_mock_app_config())
def test_ingestion_runs_upload_then_raw2plain(mock_app, mock_db):
    spark = MagicMock()
    upload_fn = MagicMock()
    raw2plain_fn = MagicMock()

    UploadScriptFactory.register("avalanche_org_raw", upload_fn)
    Raw2PlainScriptFactory.register("avalanche_org_raw", raw2plain_fn)

    from jobs.ingestion import run

    result = run(spark, "avalanche_org_raw")

    assert upload_fn.call_count == 1
    assert raw2plain_fn.call_count == 1
    assert upload_fn.call_args[0][0] is spark
    assert raw2plain_fn.call_args[0][0] is spark
    assert "complete" in result


@patch("jobs.ingestion.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
@patch("jobs.ingestion.AppConfig.from_json", return_value=_mock_app_config())
def test_ingestion_upload_before_raw2plain(mock_app, mock_db):
    spark = MagicMock()
    call_order = []
    upload_fn = MagicMock(side_effect=lambda s, c, d, **kw: call_order.append("upload"))
    raw2plain_fn = MagicMock(side_effect=lambda s, c, d, **kw: call_order.append("raw2plain"))

    UploadScriptFactory.register("t", upload_fn)
    Raw2PlainScriptFactory.register("t", raw2plain_fn)

    from jobs.ingestion import run

    run(spark, "t")

    assert call_order == ["upload", "raw2plain"]


@patch("jobs.ingestion.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
@patch("jobs.ingestion.AppConfig.from_json", return_value=_mock_app_config())
def test_ingestion_passes_config_and_db_config(mock_app, mock_db):
    spark = MagicMock()
    upload_fn = MagicMock()
    raw2plain_fn = MagicMock()

    UploadScriptFactory.register("t", upload_fn)
    Raw2PlainScriptFactory.register("t", raw2plain_fn)

    from jobs.ingestion import run

    run(spark, "t")

    app_cfg = upload_fn.call_args[0][1]
    db_cfg = upload_fn.call_args[0][2]
    assert app_cfg is mock_app.return_value
    assert db_cfg is mock_db.return_value


@patch("jobs.ingestion.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
@patch("jobs.ingestion.AppConfig.from_json", return_value=_mock_app_config())
def test_ingestion_passes_logger_to_upload_and_raw2plain(mock_app, mock_db):
    spark = MagicMock()
    upload_fn = MagicMock()
    raw2plain_fn = MagicMock()

    UploadScriptFactory.register("t", upload_fn)
    Raw2PlainScriptFactory.register("t", raw2plain_fn)

    log = MagicMock()
    from jobs.ingestion import run

    run(spark, "t", logger=log)

    assert upload_fn.call_args.kwargs["logger"] is log
    assert raw2plain_fn.call_args.kwargs["logger"] is log


@patch("jobs.ingestion.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
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
