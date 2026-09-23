import datetime as dt
from unittest.mock import MagicMock, patch

import pytest


class TestParseFileDate:
    def test_valid_filename(self):
        from upload.openmeteo_flood_upload import _parse_file_date
        assert _parse_file_date("/data/openmeteo_rivers_2026-09-01.csv") == dt.date(2026, 9, 1)

    def test_nested_path(self):
        from upload.openmeteo_flood_upload import _parse_file_date
        assert _parse_file_date("/some/deep/path/openmeteo_rivers_2025-12-31.csv") == dt.date(2025, 12, 31)

    def test_no_date_returns_none(self):
        from upload.openmeteo_flood_upload import _parse_file_date
        assert _parse_file_date("/data/random_file.csv") is None

    def test_invalid_date_returns_none(self):
        from upload.openmeteo_flood_upload import _parse_file_date
        assert _parse_file_date("/data/openmeteo_rivers_not-a-date.csv") is None

    def test_non_csv_returns_none(self):
        from upload.openmeteo_flood_upload import _parse_file_date
        assert _parse_file_date("/data/openmeteo_rivers_2026-09-01.txt") is None


class TestGetMaxFileDate:
    def test_returns_date_when_row_exists(self):
        from upload.openmeteo_flood_upload import _get_max_file_date
        spark = MagicMock()
        spark.read.jdbc.return_value.first.return_value = MagicMock(max_file_date=dt.date(2026, 9, 5))
        result = _get_max_file_date(spark, "jdbc:postgresql://x:5432/db", {}, "geo")
        assert result == dt.date(2026, 9, 5)

    def test_returns_none_when_row_is_null(self):
        from upload.openmeteo_flood_upload import _get_max_file_date
        spark = MagicMock()
        spark.read.jdbc.return_value.first.return_value = MagicMock(max_file_date=None)
        result = _get_max_file_date(spark, "jdbc:postgresql://x:5432/db", {}, "geo")
        assert result is None

    def test_returns_none_on_exception(self):
        from upload.openmeteo_flood_upload import _get_max_file_date
        spark = MagicMock()
        spark.read.jdbc.side_effect = Exception("table not found")
        result = _get_max_file_date(spark, "jdbc:postgresql://x:5432/db", {}, "geo")
        assert result is None


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


@patch("upload.openmeteo_flood_upload.F", _mock_F())
@patch("upload.openmeteo_flood_upload.StructType", MagicMock())
@patch("upload.openmeteo_flood_upload.setup_logger")
@patch("upload.openmeteo_flood_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.openmeteo_flood_upload.glob.glob")
@patch("upload.openmeteo_flood_upload.os.path.exists", return_value=True)
def test_historical_when_table_empty(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = [
        "/data/OpenMeteo_Flood/openmeteo_rivers_2026-09-01.csv",
        "/data/OpenMeteo_Flood/openmeteo_rivers_2026-09-02.csv",
    ]

    spark, df = _mock_spark(max_file_date=None)

    from upload.openmeteo_flood_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config())

    assert spark.read.csv.call_count == 2
    df.write.jdbc.assert_called_once()


@patch("upload.openmeteo_flood_upload.F", _mock_F())
@patch("upload.openmeteo_flood_upload.StructType", MagicMock())
@patch("upload.openmeteo_flood_upload.setup_logger")
@patch("upload.openmeteo_flood_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.openmeteo_flood_upload.glob.glob")
@patch("upload.openmeteo_flood_upload.os.path.exists", return_value=True)
def test_incremental_filters_by_date(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = [
        "/data/OpenMeteo_Flood/openmeteo_rivers_2026-09-01.csv",
        "/data/OpenMeteo_Flood/openmeteo_rivers_2026-09-02.csv",
        "/data/OpenMeteo_Flood/openmeteo_rivers_2026-09-03.csv",
    ]

    spark, df = _mock_spark(max_file_date=dt.date(2026, 9, 1))

    from upload.openmeteo_flood_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config())

    assert spark.read.csv.call_count == 2


@patch("upload.openmeteo_flood_upload.F", _mock_F())
@patch("upload.openmeteo_flood_upload.StructType", MagicMock())
@patch("upload.openmeteo_flood_upload.setup_logger")
@patch("upload.openmeteo_flood_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.openmeteo_flood_upload.glob.glob")
@patch("upload.openmeteo_flood_upload.os.path.exists", return_value=True)
def test_no_new_files_raises_error(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = ["/data/OpenMeteo_Flood/openmeteo_rivers_2026-09-01.csv"]

    spark, df = _mock_spark(max_file_date=dt.date(2026, 9, 1))

    from upload.openmeteo_flood_upload import upload
    with pytest.raises(ValueError, match="No new files to upload"):
        upload(spark, _mock_app_config(), _mock_db_config())


@patch("upload.openmeteo_flood_upload.F", _mock_F())
@patch("upload.openmeteo_flood_upload.StructType", MagicMock())
@patch("upload.openmeteo_flood_upload.setup_logger")
@patch("upload.openmeteo_flood_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.openmeteo_flood_upload.glob.glob")
@patch("upload.openmeteo_flood_upload.os.path.exists", return_value=True)
def test_writes_to_correct_table(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = ["/data/OpenMeteo_Flood/openmeteo_rivers_2026-09-01.csv"]

    spark, df = _mock_spark(max_file_date=None)

    from upload.openmeteo_flood_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config())

    write_args = df.write.jdbc.call_args
    assert write_args.kwargs["table"] == "geo.openmeteo_flood_raw"
    assert write_args.kwargs["mode"] == "append"


@patch("upload.openmeteo_flood_upload.F", _mock_F())
@patch("upload.openmeteo_flood_upload.StructType", MagicMock())
@patch("upload.openmeteo_flood_upload.setup_logger")
@patch("upload.openmeteo_flood_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.openmeteo_flood_upload.glob.glob")
@patch("upload.openmeteo_flood_upload.os.path.exists", return_value=True)
def test_empty_folder_raises_error(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = []

    spark, df = _mock_spark(max_file_date=None)

    from upload.openmeteo_flood_upload import upload
    with pytest.raises(ValueError, match="No new files to upload"):
        upload(spark, _mock_app_config(), _mock_db_config())


@patch("upload.openmeteo_flood_upload.F", _mock_F())
@patch("upload.openmeteo_flood_upload.StructType", MagicMock())
@patch("upload.openmeteo_flood_upload.setup_logger")
@patch("upload.openmeteo_flood_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.openmeteo_flood_upload.glob.glob")
@patch("upload.openmeteo_flood_upload.os.path.exists", return_value=True)
def test_uses_passed_logger(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = ["/data/OpenMeteo_Flood/openmeteo_rivers_2026-09-01.csv"]

    spark, df = _mock_spark(max_file_date=None)
    passed_logger = MagicMock()

    from upload.openmeteo_flood_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config(), logger=passed_logger)

    mock_logger.assert_not_called()
    passed_logger.info.assert_any_call("---- Upload started: openmeteo_flood_raw ----")


@patch("upload.openmeteo_flood_upload.F", _mock_F())
@patch("upload.openmeteo_flood_upload.StructType", MagicMock())
@patch("upload.openmeteo_flood_upload.setup_logger")
@patch("upload.openmeteo_flood_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.openmeteo_flood_upload.glob.glob")
@patch("upload.openmeteo_flood_upload.os.path.exists", return_value=True)
def test_creates_own_logger_when_none(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = ["/data/OpenMeteo_Flood/openmeteo_rivers_2026-09-01.csv"]

    spark, df = _mock_spark(max_file_date=None)

    from upload.openmeteo_flood_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config(), logger=None)

    mock_logger.assert_called_once()
