from unittest.mock import MagicMock, patch

from test.conftest import mock_app_config, mock_db_config


@patch("run_drop_table.SparkSession.builder")
@patch("run_drop_table.drop_table")
@patch("run_drop_table.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("run_drop_table.AppConfig.from_json", return_value=mock_app_config(logging=True, spark="geo_etl"))
@patch("run_drop_table.setup_logger")
def test_drop_table_calls_correct_functions(mock_logger, mock_app, mock_db, mock_drop, mock_builder):
    import sys

    sys.argv = ["run_drop_table.py", "my_table"]

    from run_drop_table import main

    main()

    mock_drop.assert_called_once()
    args = mock_drop.call_args[0]
    assert args[1] == "my_table"
    assert args[2] == "geo"


@patch("run_drop_table.SparkSession.builder")
@patch("run_drop_table.drop_table")
@patch("run_drop_table.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("run_drop_table.AppConfig.from_json", return_value=mock_app_config(logging=True, spark="geo_etl"))
@patch("run_drop_table.setup_logger")
def test_drop_table_logs_success(mock_logger, mock_app, mock_db, mock_drop, mock_builder):
    import sys

    sys.argv = ["run_drop_table.py", "my_table"]

    mock_log = MagicMock()
    mock_logger.return_value = mock_log

    from run_drop_table import main

    main()

    mock_log.info.assert_any_call("Table geo.my_table dropped.")


@patch("run_drop_table.SparkSession.builder")
@patch("run_drop_table.drop_table", side_effect=RuntimeError("boom"))
@patch("run_drop_table.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("run_drop_table.AppConfig.from_json", return_value=mock_app_config(logging=True, spark="geo_etl"))
@patch("run_drop_table.setup_logger")
def test_drop_table_failure_is_reraised(mock_logger, mock_app, mock_db, mock_drop, mock_builder):
    import sys

    import pytest

    sys.argv = ["run_drop_table.py", "my_table"]

    from run_drop_table import main

    with pytest.raises(RuntimeError, match="boom"):
        main()
