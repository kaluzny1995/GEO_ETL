"""Parametrized run-behaviour tests shared by all ten single-ETL scripts.

Each case exercises the same behaviour against a different script class —
the script supplies its own source/target tables while
:func:`utils.etl_utils.run_etl` provides the shared runner.
"""

import datetime as dt
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from model.enums import LoadType
from single_etl.avalanche_org_etl import AvalancheOrgEtlScript
from single_etl.eonet_event_etl import EonetEventEtlScript
from single_etl.gdacs_event_etl import GdacsEventEtlScript
from single_etl.ibtracs_cyclone_etl import IbtracsCycloneEtlScript
from single_etl.nifc_fire_etl import NifcFireEtlScript
from single_etl.openmeteo_flood_etl import OpenMeteoFloodEtlScript
from single_etl.union_etl import UnionEtlScript
from single_etl.usgs_earthquake_etl import UsgsEarthquakeEtlScript
from single_etl.usgs_nwis_etl import UsgsNwisEtlScript
from single_etl.usgs_volcano_etl import UsgsVolcanoEtlScript
from test.conftest import mock_app_config, mock_db_config

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
    (UnionEtlScript, tuple(f"geo.{table}" for table in UnionEtlScript.SOURCE_TABLE), "geo.union_etl"),
]

INCREMENTAL_CASES = [
    (AvalancheOrgEtlScript, "geo.avalanche_org", "geo.avalanche_org_etl"),
    (EonetEventEtlScript, "geo.eonet_event", "geo.eonet_event_etl"),
    (GdacsEventEtlScript, "geo.gdacs_event", "geo.gdacs_event_etl"),
    (IbtracsCycloneEtlScript, "geo.ibtracs_cyclone", "geo.ibtracs_cyclone_etl"),
]


def _spark_with_rows():
    """One frame for every read — filter and stacking return it unchanged.

    The union script stacks nine reads with ``unionByName``; collapsing each
    frame back onto itself keeps every assertion (head, convert input) valid
    for one source and for nine.
    """
    spark = MagicMock()
    frame = spark.read.jdbc.return_value
    frame.filter.return_value = frame
    frame.unionByName.return_value = frame
    return spark


@contextmanager
def _patch_convert(script_cls, **kwargs):
    """Patch ``_convert`` and keep the mocked frame flowing through dedup.

    The runner deduplicates the converted frame with ``dropDuplicates``;
    pointing that call back at the same mock keeps every write assertion on
    one object, just as ``_spark_with_rows`` collapses the read frames.
    """
    with patch.object(script_cls, "_convert", **kwargs) as mock_convert:
        converted = mock_convert.return_value
        converted.dropDuplicates.return_value = converted
        yield mock_convert


class TestRun:
    @pytest.mark.parametrize("script_cls, source_table, target_table", RUN_CASES)
    def test_auto_detects_historical(self, script_cls, source_table, target_table):
        # Arrange
        spark = _spark_with_rows()

        # Act
        with (
            _patch_convert(script_cls) as mock_convert,
            patch("utils.etl_utils.get_max_file_date", return_value=None),
        ):
            outcome = script_cls().run(spark, mock_app_config(), mock_db_config(driver=False))

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
            _patch_convert(script_cls) as mock_convert,
            patch("utils.etl_utils.get_max_file_date", return_value=dt.date(2026, 9, 28)),
        ):
            outcome = script_cls().run(spark, mock_app_config(), mock_db_config(driver=False))

        # Assert
        assert outcome.loaded is True
        assert outcome.summary.startswith("INCREMENTAL")
        expected_sources = [source_table] if isinstance(source_table, str) else list(source_table)
        assert spark.read.jdbc.return_value.filter.call_count == len(expected_sources)
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
            _patch_convert(script_cls) as mock_convert,
            patch("utils.etl_utils.get_max_file_date", return_value=dt.date(2026, 9, 28)),
        ):
            outcome = script_cls().run(
                spark, mock_app_config(), mock_db_config(driver=False), load_type=LoadType.HISTORICAL
            )

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
                mock_app_config(),
                mock_db_config(driver=False),
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
            outcome = script_cls().run(spark, mock_app_config(), mock_db_config(driver=False))

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
            outcome = script_cls().run(spark, mock_app_config(), mock_db_config(driver=False))

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
            script_cls().run(spark, mock_app_config(), mock_db_config(driver=False))

        # Assert
        expected_sources = [source_table] if isinstance(source_table, str) else list(source_table)
        assert [call.kwargs["table"] for call in spark.read.jdbc.call_args_list] == expected_sources
        assert mock_convert.call_args.args[0] is spark.read.jdbc.return_value.filter.return_value


class TestStackedProvenance:
    """The union stacks nine tables, each keyed by its own ``source_name``.

    The per-source ``source_name`` threshold is what keeps INCREMENTAL
    honest — a single global ``max(file_date)`` would silently skip the
    older files of a source that catches up after the others (the 51-row
    IBTRACS gap this behaviour caused in production).
    """

    def test_incremental_thresholds_are_per_source_table(self):
        # Arrange — the union holds rows up to 2026-10-03, but IBTRACS only
        # up to 2026-10-01: every source must be filtered with ITS date.
        global_max = dt.date(2026, 10, 3)
        stale = "ibtracs_cyclone_etl"
        per_source = {
            table: dt.date(2026, 10, 1) if table == stale else global_max for table in UnionEtlScript.SOURCE_TABLE
        }
        spark = _spark_with_rows()

        def fake_max_file_date(*_args, source_table=None, **_kwargs):
            if source_table is None:
                return global_max
            return per_source[source_table]

        # Act
        with (
            patch.object(UnionEtlScript, "_convert"),
            patch("utils.etl_utils.get_max_file_date", side_effect=fake_max_file_date) as mock_max,
        ):
            outcome = UnionEtlScript().run(spark, mock_app_config(), mock_db_config(driver=False))

        # Assert — one global query (load type) + one threshold query per source
        assert outcome.loaded is True
        assert mock_max.call_count == 1 + len(UnionEtlScript.SOURCE_TABLE)
        assert mock_max.call_args_list[0].kwargs.get("source_table") is None
        conditions = [str(call.args[0]) for call in spark.read.jdbc.return_value.filter.call_args_list]
        assert len(conditions) == len(UnionEtlScript.SOURCE_TABLE)
        stale_index = list(UnionEtlScript.SOURCE_TABLE).index(stale)
        assert "2026-10-01" in conditions[stale_index]
        for index, condition in enumerate(conditions):
            if index != stale_index:
                assert "2026-10-03" in condition, UnionEtlScript.SOURCE_TABLE[index]

    def test_source_absent_from_the_target_is_stacked_in_full(self):
        # Arrange — a source with no rows in the union yet must not be
        # filtered with another source's threshold (that would skip it forever).
        fresh = "gdacs_event_etl"
        global_max = dt.date(2026, 10, 3)

        def fake_max_file_date(*_args, source_table=None, **_kwargs):
            if source_table is None:
                return global_max
            return None if source_table == fresh else global_max

        spark = _spark_with_rows()
        logger = MagicMock()

        # Act
        with (
            patch.object(UnionEtlScript, "_convert"),
            patch("utils.etl_utils.get_max_file_date", side_effect=fake_max_file_date),
        ):
            outcome = UnionEtlScript().run(
                spark,
                mock_app_config(),
                mock_db_config(driver=False),
                logger=logger,
            )

        # Assert — every source except the fresh one is date-filtered
        assert outcome.loaded is True
        assert spark.read.jdbc.return_value.filter.call_count == len(UnionEtlScript.SOURCE_TABLE) - 1
        messages = [str(call.args[0]) for call in logger.info.call_args_list]
        assert any(f"Processing every row of {fresh} — it has no rows in union_etl yet" in msg for msg in messages)


class TestDeduplication:
    """Every script deduplicates the converted frame on its own keys.

    ``DEDUP_KEYS`` + ``file_date`` is what makes a run idempotent per file:
    GDACS's paginated listings and avalanche.org's conflicting standing
    products collapse to one row per event per file, while the snapshots of
    other files are untouched — and ``union_etl``, which stacks sources that
    are already deduplicated by their own scripts, declares no keys.
    """

    @pytest.mark.parametrize("script_cls, source_table, target_table", RUN_CASES)
    def test_dedup_runs_with_the_scripts_keys_scoped_to_file_date(self, script_cls, source_table, target_table):
        # Arrange
        spark = _spark_with_rows()

        # Act
        with (
            _patch_convert(script_cls) as mock_convert,
            patch("utils.etl_utils.get_max_file_date", return_value=dt.date(2026, 9, 28)),
        ):
            outcome = script_cls().run(spark, mock_app_config(), mock_db_config(driver=False))

        # Assert — the frame written is the deduplicated one, keyed per file
        assert outcome.loaded is True
        converted = mock_convert.return_value
        converted.write.jdbc.assert_called_once()
        if script_cls.DEDUP_KEYS is None:
            converted.dropDuplicates.assert_not_called()
        else:
            converted.dropDuplicates.assert_called_once_with([*script_cls.DEDUP_KEYS, "file_date"])
