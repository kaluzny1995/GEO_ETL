"""Tests for the PySpark CSV metadata publisher."""

import datetime as dt
import json
import os
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from model.enums import LoadType
from model.other.data_source_file import DataSourceFile
from test.conftest import mock_app_config_upload, mock_db_config
from utils.data_source_files import (
    PublishResult,
    collect_source_file_rows,
    configured_pipeline_start_date,
    count_csv_records,
    publish_source_files,
)


def write_csv(path: Path, rows: list[str]) -> None:
    """Write a CSV with a header and the supplied data lines."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(["id,name", *rows]) + "\n", encoding="utf-8")


def write_download_config(path: Path, sources: dict[str, dict[str, object]]) -> None:
    """Write a minimal GEO_Download config."""
    path.write_text(json.dumps({"sources": sources}), encoding="utf-8")


def file_row(
    file_name: str,
    snapshot_date: str | None,
    *,
    created_at: str = "2026-10-04T02:14:50Z",
    records: int = 2,
    size_kb: float = 0.7,
) -> DataSourceFile:
    """Build one file metadata row."""
    return DataSourceFile(
        source_name="EONET",
        file_name=file_name,
        snapshot_date=snapshot_date,
        created_at=created_at,
        records=records,
        size_kb=size_kb,
        pipeline_start_date="2026-09-01",
    )


def existing_row(
    file_name: str,
    snapshot_date: str | None,
    *,
    created_at: str = "2026-10-04T02:14:50Z",
    records: int = 2,
    size_kb: float = 0.7,
) -> SimpleNamespace:
    """Build a stored-row stand-in with the attributes the publisher compares."""
    return SimpleNamespace(
        file_name=file_name,
        snapshot_date=snapshot_date,
        created_at=created_at,
        records=records,
        size_kb=size_kb,
    )


def written_file_rows(spark: MagicMock) -> list[dict]:
    """Return the file-row batches passed to ``spark.createDataFrame``."""
    batches = [call.args[0] for call in spark.createDataFrame.call_args_list]
    return [batch for batch in batches if batch and "file_name" in batch[0]]


def written_source_rows(spark: MagicMock) -> list[dict]:
    """Return the source-row batches passed to ``spark.createDataFrame``."""
    batches = [call.args[0] for call in spark.createDataFrame.call_args_list]
    return [batch for batch in batches if batch and "updated_at" in batch[0]]


class TestCountCsvRecords:
    """Tests for the header-excluding row counter."""

    def test_excludes_header_and_blank_lines(self, tmp_path: Path) -> None:
        path = tmp_path / "data.csv"
        path.write_text("id,name\n1,alpha\n\n2,beta\n", encoding="utf-8")

        assert count_csv_records(str(path)) == 2

    def test_quoted_newlines_count_as_one_record(self, tmp_path: Path) -> None:
        path = tmp_path / "data.csv"
        path.write_text('id,description\n1,"line one\nline two"\n', encoding="utf-8")

        assert count_csv_records(str(path)) == 1


class TestConfiguredPipelineStartDate:
    """Tests for reading the earliest enabled source start date."""

    def test_uses_the_earliest_enabled_source(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "GEO_Download" / "data"
        data_dir.mkdir(parents=True)
        write_download_config(
            tmp_path / "GEO_Download" / "config.json",
            {
                "EONET": {"enabled": True, "start_date": "2026-09-01"},
                "NIFC": {"enabled": True, "start_date": "2026-08-30"},
                "DISABLED": {"enabled": False, "start_date": "2026-01-01"},
            },
        )

        assert configured_pipeline_start_date(str(data_dir)) == "2026-08-30"

    def test_missing_config_returns_none(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "GEO_Download" / "data"
        data_dir.mkdir(parents=True)

        assert configured_pipeline_start_date(str(data_dir)) is None


class TestCollectSourceFileRows:
    """Tests for one source's CSV inventory."""

    def test_includes_every_csv_but_no_source_sentinel(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "GEO_Download" / "data"
        first = data_dir / "EONET" / "eonet_events_2026-09-01.csv"
        second = data_dir / "EONET" / "eonet_events_2026-09-03.csv"
        write_csv(first, ["1,alpha", "2,beta"])
        write_csv(second, ["1,gamma"])
        (data_dir / "EONET" / "notes.txt").write_text("ignored", encoding="utf-8")
        os.utime(first, (dt.datetime(2026, 10, 4, 2, 14, 50, tzinfo=dt.UTC).timestamp(),) * 2)

        rows = collect_source_file_rows(str(data_dir), "EONET", "2026-09-01", today=dt.date(2026, 11, 3))

        assert len(rows) == 2
        first_row = rows[0]
        assert first_row.file_name == "eonet_events_2026-09-01.csv"
        assert first_row.snapshot_date == "2026-09-01"
        assert first_row.created_at == "2026-10-04T02:14:50Z"
        assert first_row.records == 2
        assert first_row.size_kb == round(first.stat().st_size / 1024, 1)
        assert first_row.pipeline_start_date == "2026-09-01"

        assert rows[1].snapshot_date == "2026-09-03"

    def test_skips_files_dated_today_or_later(self, tmp_path: Path) -> None:
        """Only dates strictly before the current date are part of the finished inventory."""
        data_dir = tmp_path / "GEO_Download" / "data"
        write_csv(data_dir / "EONET" / "eonet_events_2026-09-01.csv", ["1,alpha"])
        write_csv(data_dir / "EONET" / "eonet_events_2026-09-02.csv", ["1,beta"])
        write_csv(data_dir / "EONET" / "eonet_events_2026-09-03.csv", ["1,gamma"])

        rows = collect_source_file_rows(
            str(data_dir),
            "EONET",
            "2026-09-01",
            today=dt.date(2026, 9, 2),
        )

        assert [row.snapshot_date for row in rows] == ["2026-09-01"]

    def test_source_without_files_returns_an_empty_list(self, tmp_path: Path) -> None:
        data_dir = tmp_path / "GEO_Download" / "data"
        data_dir.mkdir(parents=True)

        assert collect_source_file_rows(str(data_dir), "EONET", "2026-09-01") == []


class TestPublishSourceFiles:
    """Tests for HISTORICAL/INCREMENTAL metadata replacement."""

    def _run(
        self,
        *,
        current_rows: list[DataSourceFile],
        existing: dict[str, SimpleNamespace],
        load_type: LoadType | None = None,
    ) -> tuple[PublishResult, MagicMock, MagicMock]:
        spark = MagicMock()
        config = mock_app_config_upload()
        db_config = mock_db_config()

        with (
            patch("utils.data_source_files.collect_source_file_rows", return_value=current_rows),
            patch("utils.data_source_files._read_existing_files", return_value=existing),
            patch("utils.data_source_files._data_dir", return_value="/data"),
            patch("utils.data_source_files.configured_pipeline_start_date", return_value="2026-09-01"),
            patch("utils.data_source_files.create_schema"),
            patch("utils.data_source_files.execute_statement") as execute_mock,
            patch("utils.data_source_files.pydantic_to_spark_schema", return_value=MagicMock()),
        ):
            result = publish_source_files(spark, config, db_config, "EONET", load_type=load_type)

        return result, spark, execute_mock

    def test_historical_replaces_every_row_when_nothing_is_stored(self) -> None:
        current = [file_row("a.csv", "2026-09-01"), file_row("b.csv", "2026-09-02")]

        result, spark, execute_mock = self._run(current_rows=current, existing={})

        assert result == PublishResult(
            source_name="EONET",
            load_type=LoadType.HISTORICAL,
            existing_files=0,
            written_files=2,
            removed_files=0,
        )
        assert written_file_rows(spark) == [[row.model_dump() for row in current]]
        assert written_source_rows(spark)[0][0]["source_name"] == "EONET"
        delete_sql = next(
            call.args[1]
            for call in execute_mock.call_args_list
            if "DELETE FROM geo.data_source_files" in str(call.args[1])
        )
        assert "WHERE source_name = 'EONET'" in delete_sql

    def test_incremental_writes_only_new_or_changed_files_and_removes_vanished(self) -> None:
        current = [
            file_row("a.csv", "2026-09-01"),
            file_row("b.csv", "2026-09-02", created_at="2026-10-05T03:00:00Z"),
            file_row("d.csv", "2026-09-04"),
        ]
        existing = {
            "a.csv": existing_row("a.csv", "2026-09-01"),
            "b.csv": existing_row("b.csv", "2026-09-02"),
            "c.csv": existing_row("c.csv", "2026-09-03"),
        }

        result, spark, execute_mock = self._run(current_rows=current, existing=existing)

        assert result.load_type == LoadType.INCREMENTAL
        assert result.existing_files == 3
        assert result.written_files == 2  # b changed, d new
        assert result.removed_files == 1  # c vanished
        written_names = [row["file_name"] for row in written_file_rows(spark)[0]]
        assert written_names == ["b.csv", "d.csv"]
        delete_sql = next(
            call.args[1]
            for call in execute_mock.call_args_list
            if "DELETE FROM geo.data_source_files" in str(call.args[1])
        )
        assert "file_name IN ('', 'b.csv', 'c.csv', 'd.csv')" in delete_sql

    def test_incremental_with_no_changes_does_not_rewrite_file_rows(self) -> None:
        current = [file_row("a.csv", "2026-09-01")]
        existing = {"a.csv": existing_row("a.csv", "2026-09-01")}

        result, spark, _execute_mock = self._run(current_rows=current, existing=existing)

        assert result.load_type == LoadType.INCREMENTAL
        assert result.written_files == 0
        assert result.removed_files == 0
        assert written_file_rows(spark) == []
        assert written_source_rows(spark)[0][0]["source_name"] == "EONET"

    def test_forced_historical_replaces_stored_rows(self) -> None:
        current = [file_row("a.csv", "2026-09-01")]
        existing = {"a.csv": existing_row("a.csv", "2026-09-01")}

        result, _spark, execute_mock = self._run(
            current_rows=current,
            existing=existing,
            load_type=LoadType.HISTORICAL,
        )

        assert result.load_type == LoadType.HISTORICAL
        assert result.written_files == 1
        delete_sql = next(
            call.args[1]
            for call in execute_mock.call_args_list
            if "DELETE FROM geo.data_source_files" in str(call.args[1])
        )
        assert "file_name IN" not in delete_sql

    def test_forced_incremental_on_an_empty_table_writes_all_files(self) -> None:
        current = [file_row("a.csv", "2026-09-01")]

        result, _spark, _execute_mock = self._run(
            current_rows=current,
            existing={},
            load_type=LoadType.INCREMENTAL,
        )

        assert result.load_type == LoadType.INCREMENTAL
        assert result.written_files == 1
