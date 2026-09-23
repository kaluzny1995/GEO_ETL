import datetime as dt
from unittest.mock import MagicMock, patch

import pytest


class TestParseFileDate:
    def test_valid_filename(self):
        from upload.eonet_event_upload import _parse_file_date
        assert _parse_file_date("/data/eonet_events_2026-09-01.csv") == dt.date(2026, 9, 1)

    def test_nested_path(self):
        from upload.eonet_event_upload import _parse_file_date
        assert _parse_file_date("/some/deep/path/eonet_events_2025-12-31.csv") == dt.date(2025, 12, 31)

    def test_no_date_returns_none(self):
        from upload.eonet_event_upload import _parse_file_date
        assert _parse_file_date("/data/random_file.csv") is None

    def test_invalid_date_returns_none(self):
        from upload.eonet_event_upload import _parse_file_date
        assert _parse_file_date("/data/eonet_events_not-a-date.csv") is None

    def test_non_csv_returns_none(self):
        from upload.eonet_event_upload import _parse_file_date
        assert _parse_file_date("/data/eonet_events_2026-09-01.txt") is None


class TestGetMaxFileDate:
    def test_returns_date_when_row_exists(self):
        from upload.eonet_event_upload import _get_max_file_date
        spark = MagicMock()
        spark.read.jdbc.return_value.first.return_value = MagicMock(max_file_date=dt.date(2026, 9, 5))
        result = _get_max_file_date(spark, "jdbc:postgresql://x:5432/db", {}, "geo")
        assert result == dt.date(2026, 9, 5)

    def test_returns_none_when_row_is_null(self):
        from upload.eonet_event_upload import _get_max_file_date
        spark = MagicMock()
        spark.read.jdbc.return_value.first.return_value = MagicMock(max_file_date=None)
        result = _get_max_file_date(spark, "jdbc:postgresql://x:5432/db", {}, "geo")
        assert result is None

    def test_returns_none_on_exception(self):
        from upload.eonet_event_upload import _get_max_file_date
        spark = MagicMock()
        spark.read.jdbc.side_effect = Exception("table not found")
        result = _get_max_file_date(spark, "jdbc:postgresql://x:5432/db", {}, "geo")
        assert result is None

    def test_uses_correct_subquery(self):
        from upload.eonet_event_upload import _get_max_file_date
        spark = MagicMock()
        spark.read.jdbc.return_value.first.return_value = MagicMock(max_file_date=None)
        _get_max_file_date(spark, "jdbc:postgresql://x:5432/db", {}, "my_schema")
        call_kwargs = spark.read.jdbc.call_args
        assert "my_schema" in call_kwargs.kwargs["table"]
        assert "MAX" in call_kwargs.kwargs["table"]
        assert "file_date" in call_kwargs.kwargs["table"]


def _mock_F():
    f_mock = MagicMock()
    f_mock.lit.return_value = MagicMock()
    return f_mock


def _mock_app_config():
    cfg = MagicMock()
    cfg.data.directory = "../GEO_Download/data"
    cfg.logging.directory = "logs"
    cfg.logging.level_int = 20
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


def _mock_spark(max_file_date=None):
    spark = MagicMock()
    spark.sparkContext.master = "spark://localhost:7078"

    df = MagicMock()
    df.count.return_value = 10
    df.withColumn.return_value = df
    df.unionByName.return_value = df

    jdbc_df = MagicMock()
    jdbc_df.first.return_value = MagicMock(max_file_date=max_file_date)

    spark.read.csv.return_value = df
    spark.read.jdbc.return_value = jdbc_df
    return spark, df


@patch("upload.eonet_event_upload.F", _mock_F())
@patch("upload.eonet_event_upload.StructType", MagicMock())
@patch("upload.eonet_event_upload.setup_logger")
@patch("upload.eonet_event_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.eonet_event_upload.glob.glob")
@patch("upload.eonet_event_upload.os.path.exists", return_value=True)
def test_historical_when_table_empty(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = [
        "/data/EONET/eonet_events_2026-09-01.csv",
        "/data/EONET/eonet_events_2026-09-02.csv",
        "/data/EONET/eonet_events_2026-09-03.csv",
    ]

    spark, df = _mock_spark(max_file_date=None)

    from upload.eonet_event_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config())

    assert spark.read.csv.call_count == 3
    df.write.jdbc.assert_called_once()


@patch("upload.eonet_event_upload.F", _mock_F())
@patch("upload.eonet_event_upload.StructType", MagicMock())
@patch("upload.eonet_event_upload.setup_logger")
@patch("upload.eonet_event_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.eonet_event_upload.glob.glob")
@patch("upload.eonet_event_upload.os.path.exists", return_value=True)
def test_incremental_filters_by_date(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = [
        "/data/EONET/eonet_events_2026-09-01.csv",
        "/data/EONET/eonet_events_2026-09-02.csv",
        "/data/EONET/eonet_events_2026-09-03.csv",
    ]

    spark, df = _mock_spark(max_file_date=dt.date(2026, 9, 1))

    from upload.eonet_event_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config())

    assert spark.read.csv.call_count == 2


@patch("upload.eonet_event_upload.F", _mock_F())
@patch("upload.eonet_event_upload.StructType", MagicMock())
@patch("upload.eonet_event_upload.setup_logger")
@patch("upload.eonet_event_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.eonet_event_upload.glob.glob")
@patch("upload.eonet_event_upload.os.path.exists", return_value=True)
def test_no_new_files_skips_upload(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = [
        "/data/EONET/eonet_events_2026-09-01.csv",
    ]

    spark, df = _mock_spark(max_file_date=dt.date(2026, 9, 1))

    from upload.eonet_event_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config())

    spark.read.csv.assert_not_called()
    df.write.jdbc.assert_not_called()


@patch("upload.eonet_event_upload.F", _mock_F())
@patch("upload.eonet_event_upload.StructType", MagicMock())
@patch("upload.eonet_event_upload.setup_logger")
@patch("upload.eonet_event_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.eonet_event_upload.glob.glob")
@patch("upload.eonet_event_upload.os.path.exists", return_value=True)
def test_logs_record_count(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = ["/data/EONET/eonet_events_2026-09-01.csv"]

    spark, df = _mock_spark(max_file_date=None)
    df.count.return_value = 42

    log = MagicMock()
    mock_logger.return_value = log

    from upload.eonet_event_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config())

    log.info.assert_any_call("Total records to upload: 42")
    log.info.assert_any_call("Upload complete: 42 records written to geo.eonet_event_raw")


@patch("upload.eonet_event_upload.F", _mock_F())
@patch("upload.eonet_event_upload.StructType", MagicMock())
@patch("upload.eonet_event_upload.setup_logger")
@patch("upload.eonet_event_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.eonet_event_upload.glob.glob")
@patch("upload.eonet_event_upload.os.path.exists", return_value=True)
def test_writes_to_correct_table(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = ["/data/EONET/eonet_events_2026-09-01.csv"]

    spark, df = _mock_spark(max_file_date=None)

    from upload.eonet_event_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config())

    write_args = df.write.jdbc.call_args
    assert write_args.kwargs["table"] == "geo.eonet_event_raw"
    assert write_args.kwargs["mode"] == "append"


@patch("upload.eonet_event_upload.F", _mock_F())
@patch("upload.eonet_event_upload.StructType", MagicMock())
@patch("upload.eonet_event_upload.setup_logger")
@patch("upload.eonet_event_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.eonet_event_upload.glob.glob")
@patch("upload.eonet_event_upload.os.path.exists", return_value=True)
def test_empty_folder_skips_upload(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = []

    spark, df = _mock_spark(max_file_date=None)

    from upload.eonet_event_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config())

    spark.read.csv.assert_not_called()
    df.write.jdbc.assert_not_called()


@patch("upload.eonet_event_upload.F", _mock_F())
@patch("upload.eonet_event_upload.StructType", MagicMock())
@patch("upload.eonet_event_upload.setup_logger")
@patch("upload.eonet_event_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.eonet_event_upload.glob.glob")
@patch("upload.eonet_event_upload.os.path.exists", return_value=True)
def test_uses_passed_logger(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = ["/data/EONET/eonet_events_2026-09-01.csv"]

    spark, df = _mock_spark(max_file_date=None)
    passed_logger = MagicMock()

    from upload.eonet_event_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config(), logger=passed_logger)

    mock_logger.assert_not_called()
    passed_logger.info.assert_any_call("---- Upload started: eonet_event_raw ----")


@patch("upload.eonet_event_upload.F", _mock_F())
@patch("upload.eonet_event_upload.StructType", MagicMock())
@patch("upload.eonet_event_upload.setup_logger")
@patch("upload.eonet_event_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.eonet_event_upload.glob.glob")
@patch("upload.eonet_event_upload.os.path.exists", return_value=True)
def test_creates_own_logger_when_none(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = ["/data/EONET/eonet_events_2026-09-01.csv"]

    spark, df = _mock_spark(max_file_date=None)

    from upload.eonet_event_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config(), logger=None)

    mock_logger.assert_called_once()
