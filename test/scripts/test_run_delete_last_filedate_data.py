import datetime as dt
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from test.conftest import mock_app_config, mock_db_config


@patch("run_delete_last_filedate_data.SparkSession.builder")
@patch("run_delete_last_filedate_data._get_jdbc_connection")
@patch("run_delete_last_filedate_data._get_max_file_date", return_value="2026-09-22")
@patch("run_delete_last_filedate_data.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("run_delete_last_filedate_data.AppConfig.from_json", return_value=mock_app_config(logging=True, spark="geo_etl"))
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
@patch("run_delete_last_filedate_data.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("run_delete_last_filedate_data.AppConfig.from_json", return_value=mock_app_config(logging=True, spark="geo_etl"))
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
@patch("run_delete_last_filedate_data.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("run_delete_last_filedate_data.AppConfig.from_json", return_value=mock_app_config(logging=True, spark="geo_etl"))
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
@patch("run_delete_last_filedate_data.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("run_delete_last_filedate_data.AppConfig.from_json", return_value=mock_app_config(logging=True, spark="geo_etl"))
@patch("run_delete_last_filedate_data.setup_logger")
def test_etl_suffix_deletes_only_the_etl_table(mock_logger, mock_app, mock_db, mock_max_date, mock_conn, mock_builder):
    import sys

    sys.argv = ["run_delete_last_filedate_data.py", "avalanche_org_etl"]

    conn_mock = MagicMock()
    mock_conn.return_value = conn_mock

    from run_delete_last_filedate_data import main

    main()

    # The latest file_date is read from the ETL table itself
    assert mock_max_date.call_args.args[4] == "avalanche_org_etl"

    stmt = conn_mock.createStatement.return_value
    delete_calls = [c for c in stmt.executeUpdate.call_args_list if "DELETE" in str(c)]
    assert len(delete_calls) == 1
    assert "geo.avalanche_org_etl WHERE" in str(delete_calls[0])
    assert "geo.avalanche_org_raw" not in str(delete_calls[0])


@patch("run_delete_last_filedate_data.SparkSession.builder")
@patch("run_delete_last_filedate_data._get_jdbc_connection")
@patch("run_delete_last_filedate_data._get_max_file_date", return_value=None)
@patch("run_delete_last_filedate_data.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("run_delete_last_filedate_data.AppConfig.from_json", return_value=mock_app_config(logging=True, spark="geo_etl"))
@patch("run_delete_last_filedate_data.setup_logger")
def test_etl_suffix_skips_when_no_data(mock_logger, mock_app, mock_db, mock_max_date, mock_conn, mock_builder):
    import sys

    sys.argv = ["run_delete_last_filedate_data.py", "avalanche_org_etl"]

    from run_delete_last_filedate_data import main

    main()

    mock_conn.assert_not_called()


@pytest.mark.parametrize("arg", ["data_quality", "data_quality_raw", "data_quality_etl"])
@patch("run_delete_last_filedate_data.SparkSession.builder")
@patch("run_delete_last_filedate_data._get_jdbc_connection")
@patch("run_delete_last_filedate_data._get_max_file_date", return_value="2026-09-22")
@patch("run_delete_last_filedate_data.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("run_delete_last_filedate_data.AppConfig.from_json", return_value=mock_app_config(logging=True, spark="geo_etl"))
@patch("run_delete_last_filedate_data.setup_logger")
def test_rejects_the_data_quality_table(mock_logger, mock_app, mock_db, mock_max_date, mock_conn, mock_builder, arg):
    import sys

    from run_delete_last_filedate_data import main

    sys.argv = ["run_delete_last_filedate_data.py", arg]
    with pytest.raises(ValueError, match="data_quality"):
        main()

    mock_conn.assert_not_called()
    mock_max_date.assert_not_called()
    mock_builder.master.assert_not_called()


@patch("run_delete_last_filedate_data.SparkSession.builder")
@patch("run_delete_last_filedate_data._get_jdbc_connection")
@patch("run_delete_last_filedate_data._get_max_file_date", return_value="2026-09-22")
@patch("run_delete_last_filedate_data.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("run_delete_last_filedate_data.AppConfig.from_json", return_value=mock_app_config(logging=True, spark="geo_etl"))
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
@patch("run_delete_last_filedate_data.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("run_delete_last_filedate_data.AppConfig.from_json", return_value=mock_app_config(logging=True, spark="geo_etl"))
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
@patch("run_delete_last_filedate_data.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("run_delete_last_filedate_data.AppConfig.from_json", return_value=mock_app_config(logging=True, spark="geo_etl"))
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


def test_delete_file_date_raises_when_count_query_returns_no_row():
    from run_delete_last_filedate_data import _delete_file_date

    # Arrange
    spark = MagicMock()
    spark.read.jdbc.return_value.first.return_value = None

    # Act & Assert
    with pytest.raises(RuntimeError, match="COUNT returned no row"):
        _delete_file_date(spark, "jdbc:postgresql://db:5432/postgres", {}, "geo", "avalanche_org", "2026-09-22")


@pytest.mark.parametrize(
    "first_row, expected",
    [
        (SimpleNamespace(max_file_date=dt.date(2026, 9, 22)), "2026-09-22"),
        (None, None),
        (SimpleNamespace(max_file_date=None), None),
    ],
)
def test_get_max_file_date_should_return_expected_value(first_row, expected):
    from run_delete_last_filedate_data import _get_max_file_date

    # Arrange
    spark = MagicMock()
    spark.read.jdbc.return_value.first.return_value = first_row

    # Act
    result = _get_max_file_date(spark, "jdbc:postgresql://db:5432/postgres", {}, "geo", "avalanche_org")

    # Assert
    assert result == expected


def test_get_max_file_date_should_return_none_on_error():
    from run_delete_last_filedate_data import _get_max_file_date

    # Arrange
    spark = MagicMock()
    spark.read.jdbc.side_effect = RuntimeError("db down")

    # Act
    result = _get_max_file_date(spark, "jdbc:postgresql://db:5432/postgres", {}, "geo", "avalanche_org")

    # Assert
    assert result is None
