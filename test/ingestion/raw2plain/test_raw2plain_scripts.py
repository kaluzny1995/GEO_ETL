"""Parametrized behaviour tests shared by all nine raw2plain scripts.

Each case exercises the same behaviour against a different script class —
the script supplies its own source/target tables and ``_convert`` shape
while ``utils.raw2plain_utils`` provides the shared runner.
"""

import datetime as dt
from unittest.mock import MagicMock, patch

import pytest

from ingestion.raw2plain.avalanche_org_raw2plain import AvalancheOrgRaw2PlainScript
from ingestion.raw2plain.eonet_event_raw2plain import EonetEventRaw2PlainScript
from ingestion.raw2plain.gdacs_event_raw2plain import GdacsEventRaw2PlainScript
from ingestion.raw2plain.ibtracs_cyclone_raw2plain import IbtracsCycloneRaw2PlainScript
from ingestion.raw2plain.nifc_fire_raw2plain import NifcFireRaw2PlainScript
from ingestion.raw2plain.openmeteo_flood_raw2plain import OpenMeteoFloodRaw2PlainScript
from ingestion.raw2plain.usgs_earthquake_raw2plain import UsgsEarthquakeRaw2PlainScript
from ingestion.raw2plain.usgs_nwis_raw2plain import UsgsNwisRaw2PlainScript
from ingestion.raw2plain.usgs_volcano_raw2plain import UsgsVolcanoRaw2PlainScript
from test.conftest import (
    mock_app_config_raw2plain,
    mock_db_config,
    mock_F_raw2plain,
    mock_spark_raw2plain,
)

MODULE = "utils.raw2plain_utils"

SCRIPT_CASES = [
    (AvalancheOrgRaw2PlainScript, "avalanche_org"),
    (EonetEventRaw2PlainScript, "eonet_event"),
    (GdacsEventRaw2PlainScript, "gdacs_event"),
    (IbtracsCycloneRaw2PlainScript, "ibtracs_cyclone"),
    (NifcFireRaw2PlainScript, "nifc_fire"),
    (OpenMeteoFloodRaw2PlainScript, "openmeteo_flood"),
    (UsgsEarthquakeRaw2PlainScript, "usgs_earthquake"),
    (UsgsNwisRaw2PlainScript, "usgs_nwis"),
    (UsgsVolcanoRaw2PlainScript, "usgs_volcano"),
]

SCRIPT_CLASSES = [script_cls for script_cls, _stem in SCRIPT_CASES]

STEMS = [stem for _script_cls, stem in SCRIPT_CASES]

CONVERT_COLUMN_CASES = [
    (AvalancheOrgRaw2PlainScript, 17),
    (EonetEventRaw2PlainScript, 20),
    (GdacsEventRaw2PlainScript, 20),
    (IbtracsCycloneRaw2PlainScript, 48),
    (NifcFireRaw2PlainScript, 19),
    (OpenMeteoFloodRaw2PlainScript, 13),
    (UsgsEarthquakeRaw2PlainScript, 25),
    (UsgsNwisRaw2PlainScript, 11),
    (UsgsVolcanoRaw2PlainScript, 18),
]

F_MOCK = f"{MODULE}.F"


@pytest.mark.parametrize("script_cls", SCRIPT_CLASSES)
@patch(f"{MODULE}.setup_logger")
@patch(F_MOCK, mock_F_raw2plain(extra_col_methods=["__truediv__"], extra_f_methods=["when", "to_timestamp", "to_date"]))
def test_historical_when_plain_empty(mock_logger, script_cls):
    # Arrange
    script = script_cls()
    spark, raw_df, plain_df = mock_spark_raw2plain(max_file_date=None)

    # Act
    script.raw2plain(spark, mock_app_config_raw2plain(), mock_db_config())

    # Assert
    raw_df.filter.assert_not_called()
    plain_df.write.jdbc.assert_called_once()


@pytest.mark.parametrize("script_cls", SCRIPT_CLASSES)
@patch(f"{MODULE}.setup_logger")
@patch(F_MOCK, mock_F_raw2plain(extra_col_methods=["__truediv__"], extra_f_methods=["when", "to_timestamp", "to_date"]))
def test_incremental_filters_by_file_date(mock_logger, script_cls):
    # Arrange
    script = script_cls()
    spark, raw_df, _plain_df = mock_spark_raw2plain(max_file_date=dt.date(2026, 9, 1))

    # Act
    script.raw2plain(spark, mock_app_config_raw2plain(), mock_db_config())

    # Assert
    raw_df.filter.assert_called_once()


@pytest.mark.parametrize("script_cls", SCRIPT_CLASSES)
@patch(f"{MODULE}.setup_logger")
@patch(F_MOCK, mock_F_raw2plain(extra_col_methods=["__truediv__"], extra_f_methods=["when", "to_timestamp", "to_date"]))
def test_no_records_skips_transform(mock_logger, script_cls):
    # Arrange
    script = script_cls()
    spark, raw_df, plain_df = mock_spark_raw2plain(max_file_date=None)
    raw_df.head.return_value = []

    # Act
    result = script.raw2plain(spark, mock_app_config_raw2plain(), mock_db_config())

    # Assert
    assert result is False
    plain_df.write.jdbc.assert_not_called()
    assert "No new records to process" in mock_logger.return_value.warning.call_args[0][0]


@pytest.mark.parametrize("script_cls, stem", SCRIPT_CASES)
@patch(f"{MODULE}.setup_logger")
@patch(F_MOCK, mock_F_raw2plain(extra_col_methods=["__truediv__"], extra_f_methods=["when", "to_timestamp", "to_date"]))
def test_writes_to_correct_table(mock_logger, script_cls, stem):
    # Arrange
    script = script_cls()
    spark, _raw_df, plain_df = mock_spark_raw2plain(max_file_date=None)

    # Act
    result = script.raw2plain(spark, mock_app_config_raw2plain(), mock_db_config())

    # Assert
    assert result is True
    write_args = plain_df.write.jdbc.call_args
    assert write_args.kwargs["table"] == f"geo.{stem}"
    assert write_args.kwargs["mode"] == "append"


@pytest.mark.parametrize("script_cls, stem", SCRIPT_CASES)
@patch(f"{MODULE}.setup_logger")
@patch(F_MOCK, mock_F_raw2plain(extra_col_methods=["__truediv__"], extra_f_methods=["when", "to_timestamp", "to_date"]))
def test_logs_record_count(mock_logger, script_cls, stem):
    # Arrange
    script = script_cls()
    spark, raw_df, _plain_df = mock_spark_raw2plain(max_file_date=None)
    raw_df.count.return_value = 42

    # Act
    script.raw2plain(spark, mock_app_config_raw2plain(), mock_db_config())

    # Assert
    mock_logger.return_value.info.assert_any_call(f"Raw2Plain complete: written to geo.{stem}")


@pytest.mark.parametrize("script_cls, stem", SCRIPT_CASES)
@patch(f"{MODULE}.setup_logger")
@patch(F_MOCK, mock_F_raw2plain(extra_col_methods=["__truediv__"], extra_f_methods=["when", "to_timestamp", "to_date"]))
def test_reads_from_raw_table(mock_logger, script_cls, stem):
    # Arrange
    script = script_cls()
    spark, _raw_df, _plain_df = mock_spark_raw2plain(max_file_date=None)

    # Act
    script.raw2plain(spark, mock_app_config_raw2plain(), mock_db_config())

    # Assert
    jdbc_calls = [c for c in spark.read.jdbc.call_args_list if "MAX" not in str(c)]
    assert len(jdbc_calls) >= 1
    raw_table_call = jdbc_calls[0]
    assert f"geo.{stem}_raw" in str(raw_table_call)


@pytest.mark.parametrize("script_cls, expected_columns", CONVERT_COLUMN_CASES)
@patch(F_MOCK, mock_F_raw2plain(extra_col_methods=["__truediv__"], extra_f_methods=["when", "to_timestamp", "to_date"]))
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


@pytest.mark.parametrize("script_cls, stem", SCRIPT_CASES)
@patch(f"{MODULE}.setup_logger")
@patch(F_MOCK, mock_F_raw2plain(extra_col_methods=["__truediv__"], extra_f_methods=["when", "to_timestamp", "to_date"]))
def test_uses_passed_logger(mock_logger, script_cls, stem):
    # Arrange
    script = script_cls()
    spark, _raw_df, _plain_df = mock_spark_raw2plain(max_file_date=None)
    passed_logger = MagicMock()

    # Act
    script.raw2plain(spark, mock_app_config_raw2plain(), mock_db_config(), logger=passed_logger)

    # Assert
    mock_logger.assert_not_called()
    passed_logger.info.assert_any_call(f"---- Raw2Final started: {stem}_raw → {stem} ----")


@pytest.mark.parametrize("script_cls", SCRIPT_CLASSES)
@patch(f"{MODULE}.setup_logger")
@patch(F_MOCK, mock_F_raw2plain(extra_col_methods=["__truediv__"], extra_f_methods=["when", "to_timestamp", "to_date"]))
def test_creates_own_logger_when_none(mock_logger, script_cls):
    # Arrange
    script = script_cls()
    spark, _raw_df, _plain_df = mock_spark_raw2plain(max_file_date=None)

    # Act
    script.raw2plain(spark, mock_app_config_raw2plain(), mock_db_config(), logger=None)

    # Assert
    mock_logger.assert_called_once()


class TestGetMaxFileDate:
    @pytest.mark.parametrize("stem", STEMS)
    def test_returns_max_date_when_row_exists(self, stem):
        # Arrange
        spark = MagicMock()
        row = MagicMock()
        row.max_file_date = dt.date(2026, 9, 1)
        spark.read.jdbc.return_value.first.return_value = row

        from utils.raw2plain_utils import get_max_file_date

        # Act
        result = get_max_file_date(spark, "jdbc:url", {}, "geo", stem)

        # Assert
        assert result == dt.date(2026, 9, 1)

    @pytest.mark.parametrize("stem", STEMS)
    def test_returns_none_when_no_rows(self, stem):
        # Arrange
        spark = MagicMock()
        spark.read.jdbc.return_value.first.return_value = None

        from utils.raw2plain_utils import get_max_file_date

        # Act
        result = get_max_file_date(spark, "jdbc:url", {}, "geo", stem)

        # Assert
        assert result is None

    @pytest.mark.parametrize("stem", STEMS)
    def test_returns_none_when_max_file_date_is_null(self, stem):
        # Arrange
        spark = MagicMock()
        row = MagicMock()
        row.max_file_date = None
        spark.read.jdbc.return_value.first.return_value = row

        from utils.raw2plain_utils import get_max_file_date

        # Act
        result = get_max_file_date(spark, "jdbc:url", {}, "geo", stem)

        # Assert
        assert result is None

    @pytest.mark.parametrize("stem", STEMS)
    def test_returns_none_on_exception(self, stem):
        # Arrange
        spark = MagicMock()
        spark.read.jdbc.side_effect = Exception("table not found")

        from utils.raw2plain_utils import get_max_file_date

        # Act
        result = get_max_file_date(spark, "jdbc:url", {}, "geo", stem)

        # Assert
        assert result is None

    @pytest.mark.parametrize("stem", STEMS)
    def test_queries_correct_table(self, stem):
        # Arrange
        spark = MagicMock()
        spark.read.jdbc.return_value.first.return_value = None

        from utils.raw2plain_utils import get_max_file_date

        # Act
        get_max_file_date(spark, "jdbc:url", {}, "geo", stem)

        # Assert
        call_kwargs = spark.read.jdbc.call_args
        assert stem in call_kwargs.kwargs["table"]
