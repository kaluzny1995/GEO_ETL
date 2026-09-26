from unittest.mock import MagicMock, patch


def _mock_app_config():
    cfg = MagicMock()
    cfg.spark.master = "spark://localhost:7078"
    cfg.spark.app_name_prefix = "geo_etl"
    cfg.database.db_schema = "geo"
    cfg.database.jdbc_driver = "org.postgresql.Driver"
    cfg.logging.directory = "logs"
    cfg.logging.level_int = 20
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


@patch("run_delete_last_filedate_data.SparkSession.builder")
@patch("run_delete_last_filedate_data._get_jdbc_connection")
@patch("run_delete_last_filedate_data._get_max_file_date", return_value="2026-09-22")
@patch("run_delete_last_filedate_data.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
@patch("run_delete_last_filedate_data.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_delete_last_filedate_data.setup_logger")
def test_deletes_from_both_tables(mock_logger, mock_app, mock_db, mock_max_date, mock_conn, mock_builder):
    import sys

    sys.argv = ["run_delete_last_filedate_data.py", "avalanche_org"]

    conn_mock = MagicMock()
    mock_conn.return_value = conn_mock

    from run_delete_last_filedate_data import main

    main()

    # Should execute DELETE on plain table first, then raw
    stmt = conn_mock.createStatement.return_value
    delete_calls = [c for c in stmt.executeUpdate.call_args_list if "DELETE" in str(c)]
    assert len(delete_calls) == 2
    assert "geo.avalanche_org" in str(delete_calls[0])
    assert "geo.avalanche_org_raw" in str(delete_calls[1])


@patch("run_delete_last_filedate_data.SparkSession.builder")
@patch("run_delete_last_filedate_data._get_jdbc_connection")
@patch("run_delete_last_filedate_data._get_max_file_date", return_value="2026-09-22")
@patch("run_delete_last_filedate_data.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
@patch("run_delete_last_filedate_data.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_delete_last_filedate_data.setup_logger")
def test_derives_plain_table_name(mock_logger, mock_app, mock_db, mock_max_date, mock_conn, mock_builder):
    import sys

    sys.argv = ["run_delete_last_filedate_data.py", "eonet_event"]

    conn_mock = MagicMock()
    mock_conn.return_value = conn_mock

    from run_delete_last_filedate_data import main

    main()

    stmt = conn_mock.createStatement.return_value
    delete_calls = [c for c in stmt.executeUpdate.call_args_list if "DELETE" in str(c)]
    assert len(delete_calls) == 2
    assert "geo.eonet_event" in str(delete_calls[0])
    assert "geo.eonet_event_raw" in str(delete_calls[1])


@patch("run_delete_last_filedate_data.SparkSession.builder")
@patch("run_delete_last_filedate_data._get_jdbc_connection")
@patch("run_delete_last_filedate_data._get_max_file_date", return_value=None)
@patch("run_delete_last_filedate_data.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
@patch("run_delete_last_filedate_data.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_delete_last_filedate_data.setup_logger")
def test_skips_when_no_data(mock_logger, mock_app, mock_db, mock_max_date, mock_conn, mock_builder):
    import sys

    sys.argv = ["run_delete_last_filedate_data.py", "avalanche_org"]

    from run_delete_last_filedate_data import main

    main()

    mock_conn.assert_not_called()


@patch("run_delete_last_filedate_data.SparkSession.builder")
@patch("run_delete_last_filedate_data._get_jdbc_connection")
@patch("run_delete_last_filedate_data._get_max_file_date", return_value="2026-09-22")
@patch("run_delete_last_filedate_data.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
@patch("run_delete_last_filedate_data.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_delete_last_filedate_data.setup_logger")
def test_logs_delete_info(mock_logger, mock_app, mock_db, mock_max_date, mock_conn, mock_builder):
    import sys

    sys.argv = ["run_delete_last_filedate_data.py", "avalanche_org"]

    conn_mock = MagicMock()
    mock_conn.return_value = conn_mock

    log = MagicMock()
    mock_logger.return_value = log

    from run_delete_last_filedate_data import main

    main()

    log.info.assert_any_call("Latest file_date: 2026-09-22")


@patch("run_delete_last_filedate_data.SparkSession.builder")
@patch("run_delete_last_filedate_data._get_jdbc_connection")
@patch("run_delete_last_filedate_data._get_max_file_date", return_value="2026-09-22")
@patch("run_delete_last_filedate_data.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
@patch("run_delete_last_filedate_data.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_delete_last_filedate_data.setup_logger")
def test_stops_spark_after_completion(mock_logger, mock_app, mock_db, mock_max_date, mock_conn, mock_builder):
    import sys

    sys.argv = ["run_delete_last_filedate_data.py", "avalanche_org"]

    conn_mock = MagicMock()
    mock_conn.return_value = conn_mock

    from run_delete_last_filedate_data import main

    main()

    spark_instance = mock_builder.master.return_value.appName.return_value.getOrCreate.return_value
    spark_instance.stop.assert_called_once()


@patch("run_delete_last_filedate_data.SparkSession.builder")
@patch("run_delete_last_filedate_data._get_jdbc_connection")
@patch("run_delete_last_filedate_data._get_max_file_date", return_value="2026-09-22")
@patch("run_delete_last_filedate_data.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
@patch("run_delete_last_filedate_data.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_delete_last_filedate_data.setup_logger")
def test_stops_spark_on_exception(mock_logger, mock_app, mock_db, mock_max_date, mock_conn, mock_builder):
    import sys

    sys.argv = ["run_delete_last_filedate_data.py", "avalanche_org"]

    conn_mock = MagicMock()
    conn_mock.createStatement.return_value.executeUpdate.side_effect = RuntimeError("db error")
    mock_conn.return_value = conn_mock

    import pytest

    from run_delete_last_filedate_data import main

    with pytest.raises(RuntimeError, match="db error"):
        main()

    spark_instance = mock_builder.master.return_value.appName.return_value.getOrCreate.return_value
    spark_instance.stop.assert_called_once()
