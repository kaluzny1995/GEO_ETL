"""Parametrized behaviour tests shared by all nine upload scripts.

Each case exercises the same behaviour against a different script class —
the script supplies its own plain table, dataset folder and conversion while
``utils.upload_utils`` provides the shared runner.
"""

import datetime as dt
from unittest.mock import MagicMock, patch

import pytest

from ingestion.upload.avalanche_org_upload import AvalancheOrgUploadScript
from ingestion.upload.eonet_event_upload import EonetEventUploadScript
from ingestion.upload.gdacs_event_upload import GdacsEventUploadScript
from ingestion.upload.ibtracs_cyclone_upload import IbtracsCycloneUploadScript
from ingestion.upload.nifc_fire_upload import NifcFireUploadScript
from ingestion.upload.openmeteo_flood_upload import OpenMeteoFloodUploadScript
from ingestion.upload.usgs_earthquake_upload import UsgsEarthquakeUploadScript
from ingestion.upload.usgs_nwis_upload import UsgsNwisUploadScript
from ingestion.upload.usgs_volcano_upload import UsgsVolcanoUploadScript
from test.conftest import mock_app_config_upload, mock_db_config, mock_F_upload, mock_spark_upload

MODULE = "utils.upload_utils"

UPLOAD_CASES = [
    (AvalancheOrgUploadScript, "avalanche_org", "Avalanche_ORG", "avalanche_org"),
    (EonetEventUploadScript, "eonet_event", "EONET", "eonet_events"),
    (GdacsEventUploadScript, "gdacs_event", "GDACS", "gdacs"),
    (IbtracsCycloneUploadScript, "ibtracs_cyclone", "IBTrACS", "ibtracs"),
    (NifcFireUploadScript, "nifc_fire", "NIFC", "nifc"),
    (OpenMeteoFloodUploadScript, "openmeteo_flood", "OpenMeteo_Flood", "openmeteo"),
    (UsgsEarthquakeUploadScript, "usgs_earthquake", "USGS_Earthquake", "usgs_earthquakes"),
    (UsgsNwisUploadScript, "usgs_nwis", "USGS_NWIS", "usgs_streamflow"),
    (UsgsVolcanoUploadScript, "usgs_volcano", "USGS_Volcano", "usgs_volcanoes"),
]

STEMS = [stem for _script_cls, stem, _data_dir, _file_prefix in UPLOAD_CASES]

FILE_PREFIXES = [file_prefix for _script_cls, _stem, _data_dir, file_prefix in UPLOAD_CASES]

CONVERT_COLUMN_CASES = [
    (AvalancheOrgUploadScript, 18),
    (EonetEventUploadScript, 20),
    (GdacsEventUploadScript, 20),
    (IbtracsCycloneUploadScript, 48),
    (NifcFireUploadScript, 19),
    (OpenMeteoFloodUploadScript, 13),
    (UsgsEarthquakeUploadScript, 25),
    (UsgsNwisUploadScript, 13),
    (UsgsVolcanoUploadScript, 21),
]

F_MOCK = f"{MODULE}.F"
F_PATCH = mock_F_upload(extra_col_methods=["__truediv__"], extra_f_methods=["when", "to_timestamp", "to_date"])


class TestParseFileDate:
    @pytest.mark.parametrize("file_prefix", FILE_PREFIXES)
    def test_valid_filename(self, file_prefix):
        from utils.upload_utils import parse_file_date

        assert parse_file_date(f"/data/{file_prefix}_2026-09-01.csv") == dt.date(2026, 9, 1)

    @pytest.mark.parametrize("file_prefix", FILE_PREFIXES)
    def test_nested_path(self, file_prefix):
        from utils.upload_utils import parse_file_date

        assert parse_file_date(f"/some/deep/path/{file_prefix}_2025-12-31.csv") == dt.date(2025, 12, 31)

    def test_no_date_returns_none(self):
        from utils.upload_utils import parse_file_date

        assert parse_file_date("/data/random_file.csv") is None

    @pytest.mark.parametrize("file_prefix", FILE_PREFIXES)
    def test_invalid_date_returns_none(self, file_prefix):
        from utils.upload_utils import parse_file_date

        assert parse_file_date(f"/data/{file_prefix}_not-a-date.csv") is None

    @pytest.mark.parametrize("file_prefix", FILE_PREFIXES)
    def test_non_csv_returns_none(self, file_prefix):
        from utils.upload_utils import parse_file_date

        assert parse_file_date(f"/data/{file_prefix}_2026-09-01.txt") is None


class TestGetMaxFileDate:
    @pytest.mark.parametrize("stem", STEMS)
    def test_returns_date_when_row_exists(self, stem):
        from utils.upload_utils import get_max_file_date

        spark = MagicMock()
        spark.read.jdbc.return_value.first.return_value = MagicMock(max_file_date=dt.date(2026, 9, 5))
        result = get_max_file_date(spark, "jdbc:postgresql://x:5432/db", {}, "geo", stem)
        assert result == dt.date(2026, 9, 5)

    @pytest.mark.parametrize("stem", STEMS)
    def test_returns_none_when_row_is_null(self, stem):
        from utils.upload_utils import get_max_file_date

        spark = MagicMock()
        spark.read.jdbc.return_value.first.return_value = MagicMock(max_file_date=None)
        result = get_max_file_date(spark, "jdbc:postgresql://x:5432/db", {}, "geo", stem)
        assert result is None

    @pytest.mark.parametrize("stem", STEMS)
    def test_returns_none_on_exception(self, stem):
        from utils.upload_utils import get_max_file_date

        spark = MagicMock()
        spark.read.jdbc.side_effect = Exception("table not found")
        result = get_max_file_date(spark, "jdbc:postgresql://x:5432/db", {}, "geo", stem)
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
        assert "source_table" not in call_kwargs.kwargs["table"]

    @pytest.mark.parametrize("stem", STEMS)
    def test_queries_only_rows_of_the_given_source_table(self, stem):
        from utils.upload_utils import get_max_file_date

        spark = MagicMock()
        spark.read.jdbc.return_value.first.return_value = None

        get_max_file_date(spark, "jdbc:url", {}, "geo", "union_etl", source_table=f"{stem}_etl")

        subquery = spark.read.jdbc.call_args.kwargs["table"]
        assert "union_etl" in subquery
        assert f"WHERE source_name IN (SELECT DISTINCT source_name FROM geo.{stem}_etl)" in subquery


@pytest.mark.parametrize("script_cls, stem, data_dir, file_prefix", UPLOAD_CASES)
@patch(F_MOCK, F_PATCH)
@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.glob.glob")
def test_historical_when_table_empty(mock_glob, mock_logger, script_cls, stem, data_dir, file_prefix):
    # Arrange
    script = script_cls()
    mock_glob.return_value = [
        f"/data/{data_dir}/{file_prefix}_2026-09-01.csv",
        f"/data/{data_dir}/{file_prefix}_2026-09-02.csv",
        f"/data/{data_dir}/{file_prefix}_2026-09-03.csv",
    ]
    spark, df = mock_spark_upload(max_file_date=None)

    # Act
    script.upload(spark, mock_app_config_upload(), mock_db_config())

    # Assert
    assert spark.read.csv.call_count == 3
    df.select.assert_called_once()
    df.write.jdbc.assert_called_once()


@pytest.mark.parametrize("script_cls, stem, data_dir, file_prefix", UPLOAD_CASES)
@patch(F_MOCK, F_PATCH)
@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.glob.glob")
def test_incremental_filters_by_date(mock_glob, mock_logger, script_cls, stem, data_dir, file_prefix):
    # Arrange
    script = script_cls()
    mock_glob.return_value = [
        f"/data/{data_dir}/{file_prefix}_2026-09-01.csv",
        f"/data/{data_dir}/{file_prefix}_2026-09-02.csv",
        f"/data/{data_dir}/{file_prefix}_2026-09-03.csv",
    ]
    spark, _df = mock_spark_upload(max_file_date=dt.date(2026, 9, 1))

    # Act
    script.upload(spark, mock_app_config_upload(), mock_db_config())

    # Assert
    assert spark.read.csv.call_count == 2


@pytest.mark.parametrize("script_cls, stem, data_dir, file_prefix", UPLOAD_CASES)
@patch(F_MOCK, F_PATCH)
@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.glob.glob")
def test_reads_csv_as_text_with_header(mock_glob, mock_logger, script_cls, stem, data_dir, file_prefix):
    # Arrange
    script = script_cls()
    mock_glob.return_value = [f"/data/{data_dir}/{file_prefix}_2026-09-01.csv"]
    spark, _df = mock_spark_upload(max_file_date=None)

    # Act
    script.upload(spark, mock_app_config_upload(), mock_db_config())

    # Assert — no schema: every CSV column stays text until _convert casts it
    read_kwargs = spark.read.csv.call_args.kwargs
    assert read_kwargs["header"] is True
    assert "schema" not in read_kwargs


@pytest.mark.parametrize("script_cls, stem, data_dir, file_prefix", UPLOAD_CASES)
@patch(F_MOCK, F_PATCH)
@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.glob.glob")
def test_logs_record_count(mock_glob, mock_logger, script_cls, stem, data_dir, file_prefix):
    # Arrange
    script = script_cls()
    mock_glob.return_value = [f"/data/{data_dir}/{file_prefix}_2026-09-01.csv"]
    spark, _df = mock_spark_upload(max_file_date=None)
    log = MagicMock()
    mock_logger.return_value = log

    # Act
    script.upload(spark, mock_app_config_upload(), mock_db_config())

    # Assert
    log.info.assert_any_call(f"Upload complete: 1 files written to geo.{stem}")


@pytest.mark.parametrize("script_cls, stem, data_dir, file_prefix", UPLOAD_CASES)
@patch(F_MOCK, F_PATCH)
@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.glob.glob")
def test_writes_to_correct_table(mock_glob, mock_logger, script_cls, stem, data_dir, file_prefix):
    # Arrange
    script = script_cls()
    mock_glob.return_value = [f"/data/{data_dir}/{file_prefix}_2026-09-01.csv"]
    spark, df = mock_spark_upload(max_file_date=None)

    # Act
    uploaded = script.upload(spark, mock_app_config_upload(), mock_db_config())

    # Assert
    assert uploaded is True
    write_args = df.write.jdbc.call_args
    assert write_args.kwargs["table"] == f"geo.{stem}"
    assert write_args.kwargs["mode"] == "append"


@pytest.mark.parametrize("script_cls, stem, data_dir, file_prefix", UPLOAD_CASES)
@patch(F_MOCK, F_PATCH)
@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.glob.glob")
def test_no_new_files_skips_upload(mock_glob, mock_logger, script_cls, stem, data_dir, file_prefix):
    # Arrange
    script = script_cls()
    mock_glob.return_value = [f"/data/{data_dir}/{file_prefix}_2026-09-01.csv"]
    spark, _df = mock_spark_upload(max_file_date=dt.date(2026, 9, 1))

    # Act
    uploaded = script.upload(spark, mock_app_config_upload(), mock_db_config())

    # Assert
    assert uploaded is False
    spark.read.csv.assert_not_called()
    assert "No new files to upload" in mock_logger.return_value.warning.call_args[0][0]


@pytest.mark.parametrize("script_cls, stem, data_dir, file_prefix", UPLOAD_CASES)
@patch(F_MOCK, F_PATCH)
@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.glob.glob")
def test_uses_passed_logger(mock_glob, mock_logger, script_cls, stem, data_dir, file_prefix):
    # Arrange
    script = script_cls()
    mock_glob.return_value = [f"/data/{data_dir}/{file_prefix}_2026-09-01.csv"]
    spark, _df = mock_spark_upload(max_file_date=None)
    passed_logger = MagicMock()

    # Act
    script.upload(spark, mock_app_config_upload(), mock_db_config(), logger=passed_logger)

    # Assert
    mock_logger.assert_not_called()
    passed_logger.info.assert_any_call(f"---- Upload started: {stem} ----")


@pytest.mark.parametrize("script_cls, stem, data_dir, file_prefix", UPLOAD_CASES)
@patch(F_MOCK, F_PATCH)
@patch(f"{MODULE}.setup_logger")
@patch(f"{MODULE}.glob.glob")
def test_creates_own_logger_when_none(mock_glob, mock_logger, script_cls, stem, data_dir, file_prefix):
    # Arrange
    script = script_cls()
    mock_glob.return_value = [f"/data/{data_dir}/{file_prefix}_2026-09-01.csv"]
    spark, _df = mock_spark_upload(max_file_date=None)

    # Act
    script.upload(spark, mock_app_config_upload(), mock_db_config(), logger=None)

    # Assert
    mock_logger.assert_called_once()


@pytest.mark.parametrize("script_cls, expected_columns", CONVERT_COLUMN_CASES)
@patch(F_MOCK, F_PATCH)
def test_convert_selects_expected_columns(script_cls, expected_columns):
    # Arrange
    script = script_cls()
    input_df = MagicMock()
    input_df.select.return_value = MagicMock()

    # Act
    script._convert(input_df)

    # Assert
    input_df.select.assert_called_once()
    select_args = input_df.select.call_args[0]
    assert len(select_args) == expected_columns
