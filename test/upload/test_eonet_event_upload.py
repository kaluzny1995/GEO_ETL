import datetime as dt
from unittest.mock import MagicMock, patch


from test.conftest import mock_F_upload, mock_app_config_upload, mock_db_config, mock_spark_upload

MODULE = "utils.upload_utils"


class TestParseFileDate:
    def test_valid_filename(self):
        from utils.upload_utils import parse_file_date
        assert parse_file_date("/data/eonet_events_2026-09-01.csv") == dt.date(2026, 9, 1)

    def test_nested_path(self):
        from utils.upload_utils import parse_file_date
        assert parse_file_date("/some/deep/path/eonet_events_2025-12-31.csv") == dt.date(2025, 12, 31)

    def test_no_date_returns_none(self):
        from utils.upload_utils import parse_file_date
        assert parse_file_date("/data/random_file.csv") is None

    def test_invalid_date_returns_none(self):
        from utils.upload_utils import parse_file_date
        assert parse_file_date("/data/eonet_events_not-a-date.csv") is None

    def test_non_csv_returns_none(self):
        from utils.upload_utils import parse_file_date
        assert parse_file_date("/data/eonet_events_2026-09-01.txt") is None


class TestGetMaxFileDate:
    def test_returns_date_when_row_exists(self):
        from utils.upload_utils import get_max_file_date
        spark = MagicMock()
        spark.read.jdbc.return_value.first.return_value = MagicMock(max_file_date=dt.date(2026, 9, 5))
        result = get_max_file_date(spark, "jdbc:postgresql://x:5432/db", {}, "geo", "eonet_event_raw")
        assert result == dt.date(2026, 9, 5)

    def test_returns_none_when_row_is_null(self):
        from utils.upload_utils import get_max_file_date
        spark = MagicMock()
        spark.read.jdbc.return_value.first.return_value = MagicMock(max_file_date=None)
        result = get_max_file_date(spark, "jdbc:postgresql://x:5432/db", {}, "geo", "eonet_event_raw")
        assert result is None

    def test_returns_none_on_exception(self):
        from utils.upload_utils import get_max_file_date
        spark = MagicMock()
        spark.read.jdbc.side_effect = Exception("table not found")
        result = get_max_file_date(spark, "jdbc:postgresql://x:5432/db", {}, "geo", "eonet_event_raw")
        assert result is None

    def test_uses_correct_subquery(self):
        from utils.upload_utils import get_max_file_date
        spark = MagicMock()
        spark.read.jdbc.return_value.first.return_value = MagicMock(max_file_date=None)
        get_max_file_date(spark, "jdbc:postgresql://x:5432/db", {}, "my_schema", "my_table")
        call_kwargs = spark.read.jdbc.call_args
        assert "my_schema" in call_kwargs.kwargs["table"]
        assert "my_table" in call_kwargs.kwargs["table"]
        assert "MAX" in call_kwargs.kwargs["table"]
        assert "file_date" in call_kwargs.kwargs["table"]


class TestWrapperFunctions:
    def test_parse_file_date_wraps_correctly(self):
        from upload.eonet_event_upload import _parse_file_date
        assert _parse_file_date("/data/eonet_events_2026-09-01.csv") == dt.date(2026, 9, 1)

    def test_get_max_file_date_wraps_correctly(self):
        from upload.eonet_event_upload import _get_max_file_date
        spark = MagicMock()
        spark.read.jdbc.return_value.first.return_value = MagicMock(max_file_date=dt.date(2026, 9, 5))
        result = _get_max_file_date(spark, "jdbc:postgresql://x:5432/db", {}, "geo")
        assert result == dt.date(2026, 9, 5)


@patch(f"{MODULE}.F", mock_F_upload())
@patch(f"{MODULE}.StructType", MagicMock())
@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.pydantic_to_spark_schema", return_value=MagicMock())
@patch(f"{MODULE}.glob.glob")
@patch(f"{MODULE}.os.path.exists", return_value=True)
def test_historical_when_table_empty(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = [
        "/data/EONET/eonet_events_2026-09-01.csv",
        "/data/EONET/eonet_events_2026-09-02.csv",
        "/data/EONET/eonet_events_2026-09-03.csv",
    ]

    spark, df = mock_spark_upload(max_file_date=None)

    from upload.eonet_event_upload import upload
    upload(spark, mock_app_config_upload(), mock_db_config())

    assert spark.read.csv.call_count == 3
    df.write.jdbc.assert_called_once()


@patch(f"{MODULE}.F", mock_F_upload())
@patch(f"{MODULE}.StructType", MagicMock())
@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.pydantic_to_spark_schema", return_value=MagicMock())
@patch(f"{MODULE}.glob.glob")
@patch(f"{MODULE}.os.path.exists", return_value=True)
def test_incremental_filters_by_date(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = [
        "/data/EONET/eonet_events_2026-09-01.csv",
        "/data/EONET/eonet_events_2026-09-02.csv",
        "/data/EONET/eonet_events_2026-09-03.csv",
    ]

    spark, df = mock_spark_upload(max_file_date=dt.date(2026, 9, 1))

    from upload.eonet_event_upload import upload
    upload(spark, mock_app_config_upload(), mock_db_config())

    assert spark.read.csv.call_count == 2


@patch(f"{MODULE}.F", mock_F_upload())
@patch(f"{MODULE}.StructType", MagicMock())
@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.pydantic_to_spark_schema", return_value=MagicMock())
@patch(f"{MODULE}.glob.glob")
@patch(f"{MODULE}.os.path.exists", return_value=True)
def test_logs_record_count(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = ["/data/EONET/eonet_events_2026-09-01.csv"]

    spark, df = mock_spark_upload(max_file_date=None)
    df.count.return_value = 42

    log = MagicMock()
    mock_logger.return_value = log

    from upload.eonet_event_upload import upload
    upload(spark, mock_app_config_upload(), mock_db_config())

    log.info.assert_any_call("Upload complete: 1 files written to geo.eonet_event_raw")


@patch(f"{MODULE}.F", mock_F_upload())
@patch(f"{MODULE}.StructType", MagicMock())
@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.pydantic_to_spark_schema", return_value=MagicMock())
@patch(f"{MODULE}.glob.glob")
@patch(f"{MODULE}.os.path.exists", return_value=True)
def test_writes_to_correct_table(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = ["/data/EONET/eonet_events_2026-09-01.csv"]

    spark, df = mock_spark_upload(max_file_date=None)

    from upload.eonet_event_upload import upload
    upload(spark, mock_app_config_upload(), mock_db_config())

    write_args = df.write.jdbc.call_args
    assert write_args.kwargs["table"] == "geo.eonet_event_raw"
    assert write_args.kwargs["mode"] == "append"


@patch(f"{MODULE}.F", mock_F_upload())
@patch(f"{MODULE}.StructType", MagicMock())
@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.pydantic_to_spark_schema", return_value=MagicMock())
@patch(f"{MODULE}.glob.glob")
@patch(f"{MODULE}.os.path.exists", return_value=True)
def test_no_new_files_skips_upload(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = ["/data/EONET/eonet_events_2026-09-01.csv"]

    spark, df = mock_spark_upload(max_file_date=dt.date(2026, 9, 1))

    from upload.eonet_event_upload import upload
    upload(spark, mock_app_config_upload(), mock_db_config())

    spark.read.csv.assert_not_called()
    assert "No new files to upload" in mock_logger.return_value.warning.call_args[0][0]


@patch(f"{MODULE}.F", mock_F_upload())
@patch(f"{MODULE}.StructType", MagicMock())
@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.pydantic_to_spark_schema", return_value=MagicMock())
@patch(f"{MODULE}.glob.glob")
@patch(f"{MODULE}.os.path.exists", return_value=True)
def test_uses_passed_logger(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = ["/data/EONET/eonet_events_2026-09-01.csv"]

    spark, df = mock_spark_upload(max_file_date=None)
    passed_logger = MagicMock()

    from upload.eonet_event_upload import upload
    upload(spark, mock_app_config_upload(), mock_db_config(), logger=passed_logger)

    mock_logger.assert_not_called()
    passed_logger.info.assert_any_call("---- Upload started: eonet_event_raw ----")


@patch(f"{MODULE}.F", mock_F_upload())
@patch(f"{MODULE}.StructType", MagicMock())
@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.pydantic_to_spark_schema", return_value=MagicMock())
@patch(f"{MODULE}.glob.glob")
@patch(f"{MODULE}.os.path.exists", return_value=True)
def test_creates_own_logger_when_none(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = ["/data/EONET/eonet_events_2026-09-01.csv"]

    spark, df = mock_spark_upload(max_file_date=None)

    from upload.eonet_event_upload import upload
    upload(spark, mock_app_config_upload(), mock_db_config(), logger=None)

    mock_logger.assert_called_once()
