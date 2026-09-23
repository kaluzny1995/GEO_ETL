import datetime as dt
from unittest.mock import MagicMock, patch

import pytest


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


@patch("upload.avalanche_org_upload.F", _mock_F())
@patch("upload.avalanche_org_upload.StructType", MagicMock())
@patch("upload.avalanche_org_upload.setup_logger")
@patch("upload.avalanche_org_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.avalanche_org_upload.glob.glob")
@patch("upload.avalanche_org_upload.os.path.exists", return_value=True)
def test_historical_when_table_empty(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = [
        "/data/Avalanche_ORG/avalanche_org_2026-09-01.csv",
        "/data/Avalanche_ORG/avalanche_org_2026-09-02.csv",
        "/data/Avalanche_ORG/avalanche_org_2026-09-03.csv",
    ]

    spark, df = _mock_spark(max_file_date=None)

    from upload.avalanche_org_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config())

    assert spark.read.csv.call_count == 3
    df.write.jdbc.assert_called_once()


@patch("upload.avalanche_org_upload.F", _mock_F())
@patch("upload.avalanche_org_upload.StructType", MagicMock())
@patch("upload.avalanche_org_upload.setup_logger")
@patch("upload.avalanche_org_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.avalanche_org_upload.glob.glob")
@patch("upload.avalanche_org_upload.os.path.exists", return_value=True)
def test_incremental_filters_by_date(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = [
        "/data/Avalanche_ORG/avalanche_org_2026-09-01.csv",
        "/data/Avalanche_ORG/avalanche_org_2026-09-02.csv",
        "/data/Avalanche_ORG/avalanche_org_2026-09-03.csv",
    ]

    spark, df = _mock_spark(max_file_date=dt.date(2026, 9, 1))

    from upload.avalanche_org_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config())

    assert spark.read.csv.call_count == 2


@patch("upload.avalanche_org_upload.F", _mock_F())
@patch("upload.avalanche_org_upload.StructType", MagicMock())
@patch("upload.avalanche_org_upload.setup_logger")
@patch("upload.avalanche_org_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.avalanche_org_upload.glob.glob")
@patch("upload.avalanche_org_upload.os.path.exists", return_value=True)
def test_no_new_files_skips_upload(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = [
        "/data/Avalanche_ORG/avalanche_org_2026-09-01.csv",
    ]

    spark, df = _mock_spark(max_file_date=dt.date(2026, 9, 1))

    from upload.avalanche_org_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config())

    spark.read.csv.assert_not_called()
    df.write.jdbc.assert_not_called()


@patch("upload.avalanche_org_upload.F", _mock_F())
@patch("upload.avalanche_org_upload.StructType", MagicMock())
@patch("upload.avalanche_org_upload.setup_logger")
@patch("upload.avalanche_org_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.avalanche_org_upload.glob.glob")
@patch("upload.avalanche_org_upload.os.path.exists", return_value=True)
def test_logs_record_count(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = ["/data/Avalanche_ORG/avalanche_org_2026-09-01.csv"]

    spark, df = _mock_spark(max_file_date=None)
    df.count.return_value = 42

    log = MagicMock()
    mock_logger.return_value = log

    from upload.avalanche_org_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config())

    log.info.assert_any_call("Total records to upload: 42")
    log.info.assert_any_call("Upload complete: 42 records written to geo.avalanche_org_raw")


@patch("upload.avalanche_org_upload.F", _mock_F())
@patch("upload.avalanche_org_upload.StructType", MagicMock())
@patch("upload.avalanche_org_upload.setup_logger")
@patch("upload.avalanche_org_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.avalanche_org_upload.glob.glob")
@patch("upload.avalanche_org_upload.os.path.exists", return_value=True)
def test_writes_to_correct_table(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = ["/data/Avalanche_ORG/avalanche_org_2026-09-01.csv"]

    spark, df = _mock_spark(max_file_date=None)

    from upload.avalanche_org_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config())

    write_args = df.write.jdbc.call_args
    assert write_args.kwargs["table"] == "geo.avalanche_org_raw"
    assert write_args.kwargs["mode"] == "append"


@patch("upload.avalanche_org_upload.F", _mock_F())
@patch("upload.avalanche_org_upload.StructType", MagicMock())
@patch("upload.avalanche_org_upload.setup_logger")
@patch("upload.avalanche_org_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.avalanche_org_upload.glob.glob")
@patch("upload.avalanche_org_upload.os.path.exists", return_value=True)
def test_empty_folder_skips_upload(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = []

    spark, df = _mock_spark(max_file_date=None)

    from upload.avalanche_org_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config())

    spark.read.csv.assert_not_called()
    df.write.jdbc.assert_not_called()


@patch("upload.avalanche_org_upload.F", _mock_F())
@patch("upload.avalanche_org_upload.StructType", MagicMock())
@patch("upload.avalanche_org_upload.setup_logger")
@patch("upload.avalanche_org_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.avalanche_org_upload.glob.glob")
@patch("upload.avalanche_org_upload.os.path.exists", return_value=True)
def test_uses_passed_logger(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = ["/data/Avalanche_ORG/avalanche_org_2026-09-01.csv"]

    spark, df = _mock_spark(max_file_date=None)
    passed_logger = MagicMock()

    from upload.avalanche_org_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config(), logger=passed_logger)

    mock_logger.assert_not_called()
    passed_logger.info.assert_any_call("---- Upload started: avalanche_org_raw ----")


@patch("upload.avalanche_org_upload.F", _mock_F())
@patch("upload.avalanche_org_upload.StructType", MagicMock())
@patch("upload.avalanche_org_upload.setup_logger")
@patch("upload.avalanche_org_upload.pydantic_to_spark_schema", return_value=MagicMock())
@patch("upload.avalanche_org_upload.glob.glob")
@patch("upload.avalanche_org_upload.os.path.exists", return_value=True)
def test_creates_own_logger_when_none(mock_exists, mock_glob, mock_schema, mock_logger):
    mock_glob.return_value = ["/data/Avalanche_ORG/avalanche_org_2026-09-01.csv"]

    spark, df = _mock_spark(max_file_date=None)

    from upload.avalanche_org_upload import upload
    upload(spark, _mock_app_config(), _mock_db_config(), logger=None)

    mock_logger.assert_called_once()
