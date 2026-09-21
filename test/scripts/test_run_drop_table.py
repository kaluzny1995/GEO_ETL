from unittest.mock import MagicMock, patch, call


class MockArgs:
    def __init__(self, table_name="test_table"):
        self.table_name = table_name


def _mock_app_config():
    cfg = MagicMock()
    cfg.spark.master = "spark://localhost:7078"
    cfg.database.db_schema = "geo"
    cfg.database.jdbc_driver = "org.postgresql.Driver"
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


@patch("run_drop_table.drop_table")
@patch("run_drop_table.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
@patch("run_drop_table.AppConfig.from_json", return_value=_mock_app_config())
def test_drop_table_calls_correct_functions(mock_app, mock_db, mock_drop):
    import sys
    sys.argv = ["run_drop_table.py", "--table_name", "my_table"]

    from run_drop_table import main
    main()

    mock_drop.assert_called_once()
    args = mock_drop.call_args[0]
    assert args[1] == "my_table"
    assert args[2] == "geo"


@patch("run_drop_table.drop_table")
@patch("run_drop_table.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
@patch("run_drop_table.AppConfig.from_json", return_value=_mock_app_config())
def test_drop_table_prints_success(mock_app, mock_db, mock_drop, capsys):
    import sys
    sys.argv = ["run_drop_table.py", "--table_name", "my_table"]

    from run_drop_table import main
    main()

    captured = capsys.readouterr()
    assert "Table geo.my_table dropped." in captured.out
