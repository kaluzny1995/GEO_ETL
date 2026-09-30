from datetime import date
from unittest.mock import MagicMock, patch

from model.enums import LoadType
from single_etl import template_etl


def _mock_config():
    cfg = MagicMock()
    cfg.database.db_schema = "geo"
    cfg.database.jdbc_driver = "org.postgresql.Driver"
    return cfg


def _mock_db_config():
    cfg = MagicMock()
    cfg.get_jdbc_url.return_value = "jdbc:postgresql://db.example.com:5432/postgres"
    cfg.get_jdbc_properties.return_value = {"user": "user", "password": "pass"}
    return cfg


def _spark_with_rows():
    spark = MagicMock()
    spark.read.jdbc.return_value.head.return_value = [MagicMock()]
    return spark


def test_resolve_auto_empty_target_is_historical():
    assert template_etl._resolve_load_type(None, None) is LoadType.HISTORICAL


def test_resolve_auto_populated_target_is_incremental():
    assert template_etl._resolve_load_type(date(2026, 9, 28), None) is LoadType.INCREMENTAL


def test_resolve_forced_historical_beats_populated_target():
    assert template_etl._resolve_load_type(date(2026, 9, 28), LoadType.HISTORICAL) is LoadType.HISTORICAL


def test_resolve_forced_incremental_on_empty_target_falls_back_to_historical():
    assert template_etl._resolve_load_type(None, LoadType.INCREMENTAL) is LoadType.HISTORICAL


@patch("single_etl.template_etl.get_max_file_date", return_value=None)
def test_run_auto_detects_historical(mock_max):
    spark = _spark_with_rows()

    message = template_etl.run(spark, _mock_config(), _mock_db_config())

    assert message.startswith("HISTORICAL")
    assert "wrote to geo.example_target" in message
    spark.read.jdbc.return_value.filter.assert_not_called()
    spark.read.jdbc.return_value.write.jdbc.assert_called_once()
    assert spark.read.jdbc.return_value.write.jdbc.call_args.kwargs["table"] == "geo.example_target"
    assert spark.read.jdbc.return_value.write.jdbc.call_args.kwargs["mode"] == "append"


@patch("single_etl.template_etl.get_max_file_date", return_value=date(2026, 9, 28))
def test_run_auto_detects_incremental(mock_max):
    spark = _spark_with_rows()

    message = template_etl.run(spark, _mock_config(), _mock_db_config())

    assert message.startswith("INCREMENTAL")
    spark.read.jdbc.return_value.filter.assert_called_once()
    condition = str(spark.read.jdbc.return_value.filter.call_args[0][0])
    assert "file_date" in condition
    assert "2026-09-28" in condition
    spark.read.jdbc.return_value.filter.return_value.write.jdbc.assert_called_once()


@patch("single_etl.template_etl.get_max_file_date", return_value=date(2026, 9, 28))
def test_run_forced_historical_skips_filter(mock_max):
    spark = _spark_with_rows()

    message = template_etl.run(spark, _mock_config(), _mock_db_config(), load_type=LoadType.HISTORICAL)

    assert message.startswith("HISTORICAL")
    spark.read.jdbc.return_value.filter.assert_not_called()
    spark.read.jdbc.return_value.write.jdbc.assert_called_once()


@patch("single_etl.template_etl.get_max_file_date", return_value=date(2026, 9, 28))
def test_run_incremental_without_new_rows_writes_nothing(mock_max):
    spark = _spark_with_rows()
    spark.read.jdbc.return_value.filter.return_value.head.return_value = []

    message = template_etl.run(spark, _mock_config(), _mock_db_config())

    assert message.startswith("INCREMENTAL")
    assert "nothing to do" in message
    spark.read.jdbc.return_value.filter.return_value.write.jdbc.assert_not_called()


@patch("single_etl.template_etl.get_max_file_date", return_value=date(2026, 9, 28))
def test_run_reads_source_table(mock_max):
    spark = _spark_with_rows()

    template_etl.run(spark, _mock_config(), _mock_db_config())

    assert spark.read.jdbc.call_args.kwargs["table"] == "geo.example_source"
