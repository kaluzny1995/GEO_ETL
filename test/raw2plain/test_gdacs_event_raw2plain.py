import datetime as dt
from unittest.mock import MagicMock, patch

import pytest


def _mock_app_config():
    cfg = MagicMock()
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


def _mock_F():
    f_mock = MagicMock()

    def col_side_effect(name):
        col_mock = MagicMock()
        col_mock.__gt__ = MagicMock(return_value=MagicMock())
        col_mock.isin = MagicMock(return_value=col_mock)
        col_mock.otherwise = MagicMock(return_value=col_mock)
        col_mock.alias = MagicMock(return_value=col_mock)
        return col_mock

    f_mock.col.side_effect = col_side_effect
    f_mock.lit.return_value = MagicMock()
    f_mock.when.return_value = MagicMock()
    f_mock.to_timestamp.return_value = MagicMock()
    f_mock.to_date.return_value = MagicMock()
    return f_mock


def _mock_spark(max_file_date=None):
    spark = MagicMock()
    spark.sparkContext.master = "spark://localhost:7078"

    raw_df = MagicMock()
    raw_df.count.return_value = 5
    raw_df.filter.return_value = raw_df

    plain_df = MagicMock()
    plain_df.count.return_value = 5
    raw_df.select.return_value = plain_df

    jdbc_df = MagicMock()
    jdbc_df.first.return_value = MagicMock(max_file_date=max_file_date)

    def read_side_effect(*args, **kwargs):
        table = kwargs.get("table", args[1] if len(args) > 1 else "")
        if "MAX" in str(table):
            return jdbc_df
        return raw_df

    spark.read.jdbc.side_effect = read_side_effect
    return spark, raw_df, plain_df


@patch("raw2plain.gdacs_event_raw2plain.F", _mock_F())
@patch("raw2plain.gdacs_event_raw2plain.setup_logger")
def test_historical_when_plain_empty(mock_logger):
    spark, raw_df, plain_df = _mock_spark(max_file_date=None)

    from raw2plain.gdacs_event_raw2plain import raw2plain
    raw2plain(spark, _mock_app_config(), _mock_db_config())

    raw_df.filter.assert_not_called()
    plain_df.write.jdbc.assert_called_once()


@patch("raw2plain.gdacs_event_raw2plain.F", _mock_F())
@patch("raw2plain.gdacs_event_raw2plain.setup_logger")
def test_incremental_filters_by_file_date(mock_logger):
    spark, raw_df, plain_df = _mock_spark(max_file_date=dt.date(2026, 9, 1))

    from raw2plain.gdacs_event_raw2plain import raw2plain
    raw2plain(spark, _mock_app_config(), _mock_db_config())

    raw_df.filter.assert_called_once()


@patch("raw2plain.gdacs_event_raw2plain.F", _mock_F())
@patch("raw2plain.gdacs_event_raw2plain.setup_logger")
def test_no_records_raises_error(mock_logger):
    spark, raw_df, plain_df = _mock_spark(max_file_date=None)
    raw_df.count.return_value = 0

    from raw2plain.gdacs_event_raw2plain import raw2plain
    with pytest.raises(ValueError, match="No new records to process"):
        raw2plain(spark, _mock_app_config(), _mock_db_config())


@patch("raw2plain.gdacs_event_raw2plain.F", _mock_F())
@patch("raw2plain.gdacs_event_raw2plain.setup_logger")
def test_writes_to_correct_table(mock_logger):
    spark, raw_df, plain_df = _mock_spark(max_file_date=None)

    from raw2plain.gdacs_event_raw2plain import raw2plain
    raw2plain(spark, _mock_app_config(), _mock_db_config())

    write_args = plain_df.write.jdbc.call_args
    assert write_args.kwargs["table"] == "geo.gdacs_event"
    assert write_args.kwargs["mode"] == "append"


@patch("raw2plain.gdacs_event_raw2plain.F", _mock_F())
@patch("raw2plain.gdacs_event_raw2plain.setup_logger")
def test_logs_record_count(mock_logger):
    spark, raw_df, plain_df = _mock_spark(max_file_date=None)
    raw_df.count.return_value = 42

    log = MagicMock()
    mock_logger.return_value = log

    from raw2plain.gdacs_event_raw2plain import raw2plain
    raw2plain(spark, _mock_app_config(), _mock_db_config())

    log.info.assert_any_call("Records to process: 42")
    log.info.assert_any_call("Raw2Plain complete: 42 records written to geo.gdacs_event")


@patch("raw2plain.gdacs_event_raw2plain.F", _mock_F())
@patch("raw2plain.gdacs_event_raw2plain.setup_logger")
def test_reads_from_raw_table(mock_logger):
    spark, raw_df, plain_df = _mock_spark(max_file_date=None)

    from raw2plain.gdacs_event_raw2plain import raw2plain
    raw2plain(spark, _mock_app_config(), _mock_db_config())

    jdbc_calls = [c for c in spark.read.jdbc.call_args_list if "MAX" not in str(c)]
    assert len(jdbc_calls) >= 1
    raw_table_call = jdbc_calls[0]
    assert "geo.gdacs_event_raw" in str(raw_table_call)


@patch("raw2plain.gdacs_event_raw2plain.F", _mock_F())
def test_convert_selects_18_columns():
    from raw2plain.gdacs_event_raw2plain import _convert

    input_df = MagicMock()
    input_df.select.return_value = MagicMock()

    _convert(input_df)

    input_df.select.assert_called_once()
    select_args = input_df.select.call_args[0]
    assert len(select_args) == 18


@patch("raw2plain.gdacs_event_raw2plain.F", _mock_F())
@patch("raw2plain.gdacs_event_raw2plain.setup_logger")
def test_uses_passed_logger(mock_logger):
    spark, raw_df, plain_df = _mock_spark(max_file_date=None)
    passed_logger = MagicMock()

    from raw2plain.gdacs_event_raw2plain import raw2plain
    raw2plain(spark, _mock_app_config(), _mock_db_config(), logger=passed_logger)

    mock_logger.assert_not_called()
    passed_logger.info.assert_any_call("---- Raw2Final started: gdacs_event_raw → gdacs_event ----")


@patch("raw2plain.gdacs_event_raw2plain.F", _mock_F())
@patch("raw2plain.gdacs_event_raw2plain.setup_logger")
def test_creates_own_logger_when_none(mock_logger):
    spark, raw_df, plain_df = _mock_spark(max_file_date=None)

    from raw2plain.gdacs_event_raw2plain import raw2plain
    raw2plain(spark, _mock_app_config(), _mock_db_config(), logger=None)

    mock_logger.assert_called_once()
