import datetime as dt
from unittest.mock import MagicMock, patch

from ingestion.raw2plain.eonet_event_raw2plain import EonetEventRaw2PlainScript
from test.conftest import (
    mock_app_config_raw2plain,
    mock_db_config,
    mock_F_raw2plain,
    mock_spark_raw2plain,
)

MODULE = "utils.raw2plain_utils"

_SCRIPT = EonetEventRaw2PlainScript()


@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.F", mock_F_raw2plain(extra_f_methods=["when", "to_timestamp", "to_date"]))
def test_historical_when_plain_empty(mock_logger):
    spark, raw_df, plain_df = mock_spark_raw2plain(max_file_date=None)

    _SCRIPT.raw2plain(spark, mock_app_config_raw2plain(), mock_db_config())

    raw_df.filter.assert_not_called()
    plain_df.write.jdbc.assert_called_once()


@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.F", mock_F_raw2plain(extra_f_methods=["when", "to_timestamp", "to_date"]))
def test_incremental_filters_by_file_date(mock_logger):
    spark, raw_df, _plain_df = mock_spark_raw2plain(max_file_date=dt.date(2026, 9, 1))

    _SCRIPT.raw2plain(spark, mock_app_config_raw2plain(), mock_db_config())

    raw_df.filter.assert_called_once()


@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.F", mock_F_raw2plain(extra_f_methods=["when", "to_timestamp", "to_date"]))
def test_no_records_skips_transform(mock_logger):
    spark, raw_df, plain_df = mock_spark_raw2plain(max_file_date=None)
    raw_df.head.return_value = []

    result = _SCRIPT.raw2plain(spark, mock_app_config_raw2plain(), mock_db_config())

    assert result is False
    plain_df.write.jdbc.assert_not_called()
    assert "No new records to process" in mock_logger.return_value.warning.call_args[0][0]


@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.F", mock_F_raw2plain(extra_f_methods=["when", "to_timestamp", "to_date"]))
def test_writes_to_correct_table(mock_logger):
    spark, _raw_df, plain_df = mock_spark_raw2plain(max_file_date=None)

    result = _SCRIPT.raw2plain(spark, mock_app_config_raw2plain(), mock_db_config())

    assert result is True
    write_args = plain_df.write.jdbc.call_args
    assert write_args.kwargs["table"] == "geo.eonet_event"
    assert write_args.kwargs["mode"] == "append"


@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.F", mock_F_raw2plain(extra_f_methods=["when", "to_timestamp", "to_date"]))
def test_logs_record_count(mock_logger):
    spark, raw_df, _plain_df = mock_spark_raw2plain(max_file_date=None)
    raw_df.count.return_value = 42

    _SCRIPT.raw2plain(spark, mock_app_config_raw2plain(), mock_db_config())

    mock_logger.return_value.info.assert_any_call("Raw2Plain complete: written to geo.eonet_event")


@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.F", mock_F_raw2plain(extra_f_methods=["when", "to_timestamp", "to_date"]))
def test_reads_from_raw_table(mock_logger):
    spark, _raw_df, _plain_df = mock_spark_raw2plain(max_file_date=None)

    _SCRIPT.raw2plain(spark, mock_app_config_raw2plain(), mock_db_config())

    jdbc_calls = [c for c in spark.read.jdbc.call_args_list if "MAX" not in str(c)]
    assert len(jdbc_calls) >= 1
    raw_table_call = jdbc_calls[0]
    assert "geo.eonet_event_raw" in str(raw_table_call)


@patch(f"{MODULE}.F", mock_F_raw2plain(extra_f_methods=["when", "to_timestamp", "to_date"]))
def test_convert_selects_20_columns():

    input_df = MagicMock()
    input_df.select.return_value = MagicMock()

    _SCRIPT._convert(input_df)

    input_df.select.assert_called_once()
    select_args = input_df.select.call_args[0]
    assert len(select_args) == 20


@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.F", mock_F_raw2plain(extra_f_methods=["when", "to_timestamp", "to_date"]))
def test_uses_passed_logger(mock_logger):
    spark, _raw_df, _plain_df = mock_spark_raw2plain(max_file_date=None)
    passed_logger = MagicMock()

    _SCRIPT.raw2plain(spark, mock_app_config_raw2plain(), mock_db_config(), logger=passed_logger)

    mock_logger.assert_not_called()
    passed_logger.info.assert_any_call("---- Raw2Final started: eonet_event_raw → eonet_event ----")


@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.F", mock_F_raw2plain(extra_f_methods=["when", "to_timestamp", "to_date"]))
def test_creates_own_logger_when_none(mock_logger):
    spark, _raw_df, _plain_df = mock_spark_raw2plain(max_file_date=None)

    _SCRIPT.raw2plain(spark, mock_app_config_raw2plain(), mock_db_config(), logger=None)

    mock_logger.assert_called_once()


class TestGetMaxFileDate:
    def test_returns_max_date_when_row_exists(self):
        spark = MagicMock()
        row = MagicMock()
        row.max_file_date = dt.date(2026, 9, 1)
        spark.read.jdbc.return_value.first.return_value = row

        from utils.raw2plain_utils import get_max_file_date

        result = get_max_file_date(spark, "jdbc:url", {}, "geo", "eonet_event")

        assert result == dt.date(2026, 9, 1)

    def test_returns_none_when_no_rows(self):
        spark = MagicMock()
        spark.read.jdbc.return_value.first.return_value = None

        from utils.raw2plain_utils import get_max_file_date

        result = get_max_file_date(spark, "jdbc:url", {}, "geo", "eonet_event")

        assert result is None

    def test_returns_none_when_max_file_date_is_null(self):
        spark = MagicMock()
        row = MagicMock()
        row.max_file_date = None
        spark.read.jdbc.return_value.first.return_value = row

        from utils.raw2plain_utils import get_max_file_date

        result = get_max_file_date(spark, "jdbc:url", {}, "geo", "eonet_event")

        assert result is None

    def test_returns_none_on_exception(self):
        spark = MagicMock()
        spark.read.jdbc.side_effect = Exception("table not found")

        from utils.raw2plain_utils import get_max_file_date

        result = get_max_file_date(spark, "jdbc:url", {}, "geo", "eonet_event")

        assert result is None

    def test_queries_correct_table(self):
        spark = MagicMock()
        spark.read.jdbc.return_value.first.return_value = None

        from utils.raw2plain_utils import get_max_file_date

        get_max_file_date(spark, "jdbc:url", {}, "geo", "eonet_event")

        call_kwargs = spark.read.jdbc.call_args
        assert "eonet_event" in call_kwargs.kwargs["table"]
