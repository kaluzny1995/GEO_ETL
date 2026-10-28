from unittest.mock import MagicMock, patch

from test.conftest import mock_app_config, mock_db_config


@patch("run_drop_schema.SparkSession.builder")
@patch("run_drop_schema._schema_has_tables", return_value=False)
@patch("run_drop_schema.drop_schema")
@patch("run_drop_schema.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("run_drop_schema.AppConfig.from_json", return_value=mock_app_config(logging=True, spark="geo_etl"))
@patch("run_drop_schema.setup_logger")
def test_drop_schema_when_empty(mock_logger, mock_app, mock_db, mock_drop, mock_has_tables, mock_builder):
    import sys

    sys.argv = ["run_drop_schema.py", "my_schema"]

    from run_drop_schema import main

    main()

    mock_has_tables.assert_called_once()
    mock_drop.assert_called_once()


@patch("run_drop_schema.SparkSession.builder")
@patch("run_drop_schema._schema_has_tables", return_value=True)
@patch("run_drop_schema.drop_schema")
@patch("run_drop_schema.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("run_drop_schema.AppConfig.from_json", return_value=mock_app_config(logging=True, spark="geo_etl"))
@patch("run_drop_schema.setup_logger")
def test_drop_schema_when_has_tables(mock_logger, mock_app, mock_db, mock_drop, mock_has_tables, mock_builder):
    import sys

    sys.argv = ["run_drop_schema.py", "my_schema"]

    from run_drop_schema import main

    main()

    mock_has_tables.assert_called_once()
    mock_drop.assert_not_called()


@patch("run_drop_schema.SparkSession.builder")
@patch("run_drop_schema._schema_has_tables", return_value=False)
@patch("run_drop_schema.drop_schema")
@patch("run_drop_schema.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("run_drop_schema.AppConfig.from_json", return_value=mock_app_config(logging=True, spark="geo_etl"))
@patch("run_drop_schema.setup_logger")
def test_drop_schema_logs_success(mock_logger, mock_app, mock_db, mock_drop, mock_has_tables, mock_builder):
    import sys

    sys.argv = ["run_drop_schema.py", "my_schema"]

    mock_log = MagicMock()
    mock_logger.return_value = mock_log

    from run_drop_schema import main

    main()

    mock_log.info.assert_any_call("Schema 'my_schema' dropped.")


@patch("run_drop_schema.SparkSession.builder")
@patch("run_drop_schema._schema_has_tables", return_value=True)
@patch("run_drop_schema.drop_schema")
@patch("run_drop_schema.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("run_drop_schema.AppConfig.from_json", return_value=mock_app_config(logging=True, spark="geo_etl"))
@patch("run_drop_schema.setup_logger")
def test_drop_schema_logs_warning(mock_logger, mock_app, mock_db, mock_drop, mock_has_tables, mock_builder):
    import sys

    sys.argv = ["run_drop_schema.py", "my_schema"]

    mock_log = MagicMock()
    mock_logger.return_value = mock_log

    from run_drop_schema import main

    main()

    mock_log.warning.assert_any_call("Schema 'my_schema' contains tables. Drop tables first.")


@patch("run_drop_schema.SparkSession.builder")
@patch("run_drop_schema._schema_has_tables", return_value=False)
@patch("run_drop_schema.drop_schema", side_effect=RuntimeError("boom"))
@patch("run_drop_schema.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("run_drop_schema.AppConfig.from_json", return_value=mock_app_config(logging=True, spark="geo_etl"))
@patch("run_drop_schema.setup_logger")
def test_drop_schema_failure_is_reraised(mock_logger, mock_app, mock_db, mock_drop, mock_has_tables, mock_builder):
    import sys

    import pytest

    sys.argv = ["run_drop_schema.py", "my_schema"]

    from run_drop_schema import main

    with pytest.raises(RuntimeError, match="boom"):
        main()
