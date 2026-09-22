import datetime as dt
from unittest.mock import MagicMock

from upload.avalanche_org_upload import _get_max_file_date, _parse_file_date


class TestParseFileDate:
    def test_valid_filename(self):
        assert _parse_file_date("/data/avalanche_org_2026-09-01.csv") == dt.date(2026, 9, 1)

    def test_nested_path(self):
        assert _parse_file_date("/some/deep/path/avalanche_org_2025-12-31.csv") == dt.date(2025, 12, 31)

    def test_no_date_returns_none(self):
        assert _parse_file_date("/data/random_file.csv") is None

    def test_invalid_date_returns_none(self):
        assert _parse_file_date("/data/avalanche_org_not-a-date.csv") is None

    def test_non_csv_returns_none(self):
        assert _parse_file_date("/data/avalanche_org_2026-09-01.txt") is None


class TestGetMaxFileDate:
    def test_returns_date_when_row_exists(self):
        spark = MagicMock()
        spark.read.jdbc.return_value.first.return_value = MagicMock(max_file_date=dt.date(2026, 9, 5))
        result = _get_max_file_date(spark, "jdbc:postgresql://x:5432/db", {}, "geo")
        assert result == dt.date(2026, 9, 5)

    def test_returns_none_when_row_is_null(self):
        spark = MagicMock()
        spark.read.jdbc.return_value.first.return_value = MagicMock(max_file_date=None)
        result = _get_max_file_date(spark, "jdbc:postgresql://x:5432/db", {}, "geo")
        assert result is None

    def test_returns_none_on_exception(self):
        spark = MagicMock()
        spark.read.jdbc.side_effect = Exception("table not found")
        result = _get_max_file_date(spark, "jdbc:postgresql://x:5432/db", {}, "geo")
        assert result is None

    def test_uses_correct_subquery(self):
        spark = MagicMock()
        spark.read.jdbc.return_value.first.return_value = MagicMock(max_file_date=None)
        _get_max_file_date(spark, "jdbc:postgresql://x:5432/db", {}, "my_schema")
        call_kwargs = spark.read.jdbc.call_args
        assert "my_schema" in call_kwargs.kwargs["table"]
        assert "MAX" in call_kwargs.kwargs["table"]
        assert "file_date" in call_kwargs.kwargs["table"]
