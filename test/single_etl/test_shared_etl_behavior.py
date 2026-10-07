"""Parametrized run-behaviour tests shared by all nine single-ETL scripts.

Each case exercises the same behaviour against a different script class —
the script supplies its own source/target tables while
:func:`utils.etl_utils.run_etl` provides the shared runner.
"""

import datetime as dt
from unittest.mock import MagicMock, patch

import pytest

from model.enums import LoadType
from single_etl.avalanche_org_etl import AvalancheOrgEtlScript
from single_etl.eonet_event_etl import EonetEventEtlScript
from single_etl.gdacs_event_etl import GdacsEventEtlScript
from single_etl.ibtracs_cyclone_etl import IbtracsCycloneEtlScript
from single_etl.nifc_fire_etl import NifcFireEtlScript
from single_etl.openmeteo_flood_etl import OpenMeteoFloodEtlScript
from single_etl.usgs_earthquake_etl import UsgsEarthquakeEtlScript
from single_etl.usgs_nwis_etl import UsgsNwisEtlScript
from single_etl.usgs_volcano_etl import UsgsVolcanoEtlScript

RUN_CASES = [
    (AvalancheOrgEtlScript, "geo.avalanche_org", "geo.avalanche_org_etl"),
    (EonetEventEtlScript, "geo.eonet_event", "geo.eonet_event_etl"),
    (GdacsEventEtlScript, "geo.gdacs_event", "geo.gdacs_event_etl"),
    (IbtracsCycloneEtlScript, "geo.ibtracs_cyclone", "geo.ibtracs_cyclone_etl"),
    (NifcFireEtlScript, "geo.nifc_fire", "geo.nifc_fire_etl"),
    (OpenMeteoFloodEtlScript, "geo.openmeteo_flood", "geo.openmeteo_flood_etl"),
    (UsgsEarthquakeEtlScript, "geo.usgs_earthquake", "geo.usgs_earthquake_etl"),
    (UsgsNwisEtlScript, "geo.usgs_nwis", "geo.usgs_nwis_etl"),
    (UsgsVolcanoEtlScript, "geo.usgs_volcano", "geo.usgs_volcano_etl"),
]

INCREMENTAL_CASES = [
    (AvalancheOrgEtlScript, "geo.avalanche_org", "geo.avalanche_org_etl"),
    (EonetEventEtlScript, "geo.eonet_event", "geo.eonet_event_etl"),
    (GdacsEventEtlScript, "geo.gdacs_event", "geo.gdacs_event_etl"),
    (IbtracsCycloneEtlScript, "geo.ibtracs_cyclone", "geo.ibtracs_cyclone_etl"),
]


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


class TestRun:
    @pytest.mark.parametrize("script_cls, source_table, target_table", RUN_CASES)
    def test_auto_detects_historical(self, script_cls, source_table, target_table):
        # Arrange
        spark = _spark_with_rows()

        # Act
        with (
            patch.object(script_cls, "_convert") as mock_convert,
            patch("utils.etl_utils.get_max_file_date", return_value=None),
        ):
            outcome = script_cls().run(spark, _mock_config(), _mock_db_config())

        # Assert
        assert outcome.loaded is True
        assert outcome.summary.startswith("HISTORICAL")
        assert f"wrote to {target_table}" in outcome.summary
        spark.read.jdbc.return_value.filter.assert_not_called()
        writer = mock_convert.return_value.write.jdbc
        writer.assert_called_once()
        assert writer.call_args.kwargs["table"] == target_table
        assert writer.call_args.kwargs["mode"] == "append"

    @pytest.mark.parametrize("script_cls, source_table, target_table", INCREMENTAL_CASES)
    def test_auto_detects_incremental(self, script_cls, source_table, target_table):
        # Arrange
        spark = _spark_with_rows()

        # Act
        with (
            patch.object(script_cls, "_convert") as mock_convert,
            patch("utils.etl_utils.get_max_file_date", return_value=dt.date(2026, 9, 28)),
        ):
            outcome = script_cls().run(spark, _mock_config(), _mock_db_config())

        # Assert
        assert outcome.loaded is True
        assert outcome.summary.startswith("INCREMENTAL")
        spark.read.jdbc.return_value.filter.assert_called_once()
        condition = str(spark.read.jdbc.return_value.filter.call_args[0][0])
        assert "file_date" in condition
        assert "2026-09-28" in condition
        mock_convert.return_value.write.jdbc.assert_called_once()

    @pytest.mark.parametrize("script_cls, source_table, target_table", INCREMENTAL_CASES)
    def test_forced_historical_skips_filter(self, script_cls, source_table, target_table):
        # Arrange
        spark = _spark_with_rows()

        # Act
        with (
            patch.object(script_cls, "_convert") as mock_convert,
            patch("utils.etl_utils.get_max_file_date", return_value=dt.date(2026, 9, 28)),
        ):
            outcome = script_cls().run(spark, _mock_config(), _mock_db_config(), load_type=LoadType.HISTORICAL)

        # Assert
        assert outcome.loaded is True
        assert outcome.summary.startswith("HISTORICAL")
        spark.read.jdbc.return_value.filter.assert_not_called()
        mock_convert.return_value.write.jdbc.assert_called_once()

    @pytest.mark.parametrize("script_cls, source_table, target_table", INCREMENTAL_CASES)
    def test_forced_incremental_on_empty_target_logs_fallback(self, script_cls, source_table, target_table):
        # Arrange
        spark = _spark_with_rows()
        logger = MagicMock()

        # Act
        with patch.object(script_cls, "_convert"), patch("utils.etl_utils.get_max_file_date", return_value=None):
            outcome = script_cls().run(
                spark,
                _mock_config(),
                _mock_db_config(),
                load_type=LoadType.INCREMENTAL,
                logger=logger,
            )

        # Assert
        assert outcome.loaded is True
        assert outcome.summary.startswith("HISTORICAL")
        fallback_logs = [
            call.args[0] for call in logger.info.call_args_list if "target table is empty" in str(call.args[0])
        ]
        assert fallback_logs

    @pytest.mark.parametrize("script_cls, source_table, target_table", RUN_CASES)
    def test_incremental_without_new_rows_reports_not_loaded(self, script_cls, source_table, target_table):
        # Arrange
        spark = _spark_with_rows()
        spark.read.jdbc.return_value.filter.return_value.head.return_value = []

        # Act
        with (
            patch.object(script_cls, "_convert") as mock_convert,
            patch("utils.etl_utils.get_max_file_date", return_value=dt.date(2026, 9, 28)),
        ):
            outcome = script_cls().run(spark, _mock_config(), _mock_db_config())

        # Assert — the script reports the empty run, jobs/single_etl.py raises
        assert outcome.loaded is False
        assert outcome.summary.startswith("No records loaded into")
        assert target_table.split(".")[-1] in outcome.summary
        mock_convert.assert_not_called()
        mock_convert.return_value.write.jdbc.assert_not_called()

    @pytest.mark.parametrize("script_cls, source_table, target_table", RUN_CASES)
    def test_conversion_yielding_no_rows_reports_not_loaded(self, script_cls, source_table, target_table):
        # Arrange
        spark = _spark_with_rows()
        converted = MagicMock()
        converted.head.return_value = []

        # Act
        with (
            patch.object(script_cls, "_convert", return_value=converted) as mock_convert,
            patch("utils.etl_utils.get_max_file_date", return_value=dt.date(2026, 9, 28)),
        ):
            outcome = script_cls().run(spark, _mock_config(), _mock_db_config())

        # Assert
        assert outcome.loaded is False
        assert "converted to zero rows" in outcome.summary
        mock_convert.assert_called_once()
        converted.write.jdbc.assert_not_called()

    @pytest.mark.parametrize("script_cls, source_table, target_table", RUN_CASES)
    def test_reads_source_table(self, script_cls, source_table, target_table):
        # Arrange
        spark = _spark_with_rows()

        # Act
        with (
            patch.object(script_cls, "_convert") as mock_convert,
            patch("utils.etl_utils.get_max_file_date", return_value=dt.date(2026, 9, 28)),
        ):
            script_cls().run(spark, _mock_config(), _mock_db_config())

        # Assert
        assert spark.read.jdbc.call_args.kwargs["table"] == source_table
        assert mock_convert.call_args.args[0] is spark.read.jdbc.return_value.filter.return_value
