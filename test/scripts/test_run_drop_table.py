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


@patch("run_drop_table.SparkSession.builder")
@patch("run_drop_table.drop_table")
@patch("run_drop_table.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
@patch("run_drop_table.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_drop_table.setup_logger")
def test_drop_table_calls_correct_functions(mock_logger, mock_app, mock_db, mock_drop, mock_builder):
    import sys
    sys.argv = ["run_drop_table.py", "--table_name", "my_table"]

    from run_drop_table import main
    main()

    mock_drop.assert_called_once()
    args = mock_drop.call_args[0]
    assert args[1] == "my_table"
    assert args[2] == "geo"


@patch("run_drop_table.SparkSession.builder")
@patch("run_drop_table.drop_table")
@patch("run_drop_table.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
@patch("run_drop_table.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_drop_table.setup_logger")
def test_drop_table_logs_success(mock_logger, mock_app, mock_db, mock_drop, mock_builder):
    import sys
    sys.argv = ["run_drop_table.py", "--table_name", "my_table"]

    mock_log = MagicMock()
    mock_logger.return_value = mock_log

    from run_drop_table import main
    main()

    mock_log.info.assert_any_call("Table geo.my_table dropped.")
