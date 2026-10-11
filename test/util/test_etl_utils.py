import datetime as dt

from model.enums import LoadType
from utils.etl_utils import dedup, resolve_load_type


class TestResolveLoadType:
    def test_auto_empty_target_is_historical(self):
        assert resolve_load_type(None, None) is LoadType.HISTORICAL

    def test_auto_populated_target_is_incremental(self):
        assert resolve_load_type(dt.date(2026, 9, 28), None) is LoadType.INCREMENTAL

    def test_forced_historical_beats_populated_target(self):
        assert resolve_load_type(dt.date(2026, 9, 28), LoadType.HISTORICAL) is LoadType.HISTORICAL

    def test_forced_incremental_on_empty_target_falls_back_to_historical(self):
        assert resolve_load_type(None, LoadType.INCREMENTAL) is LoadType.HISTORICAL


class TestDedup:
    """``dedup`` collapses each (keys, file_date) group to one row."""

    SCHEMA = "event_name string, latitude double, metric_value double, file_date string"

    def _frame(self, spark_session):
        rows = [
            ("EQ", 1.0, 5.0, "2026-10-01"),
            ("EQ", 1.0, 5.0, "2026-10-01"),  # exact copy of the first row
            ("EQ", 1.0, 7.0, "2026-10-01"),  # same event, different payload
            ("EQ", 1.0, 5.0, "2026-10-02"),  # the same event in another file
            ("EQ", 2.0, 5.0, "2026-10-01"),  # a different event
        ]
        return spark_session.createDataFrame(rows, self.SCHEMA)

    def test_collapses_rows_sharing_the_keys_within_one_file_date(self, spark_session):
        result = dedup(self._frame(spark_session), ["event_name", "latitude"])

        assert result.count() == 3

    def test_keeps_the_snapshots_of_other_file_dates(self, spark_session):
        source = self._frame(spark_session)
        result = dedup(source, ["event_name", "latitude"])

        keyed = result.select("event_name", "latitude", "file_date")
        snapshots = {(row.event_name, row.latitude, row.file_date) for row in keyed.collect()}
        assert snapshots == {("EQ", 1.0, "2026-10-01"), ("EQ", 1.0, "2026-10-02"), ("EQ", 2.0, "2026-10-01")}

    def test_keeps_the_converted_column_order(self, spark_session):
        source = self._frame(spark_session)
        result = dedup(source, ["event_name", "latitude"])

        assert result.columns == source.columns

    def test_null_key_values_are_treated_as_equal(self, spark_session):
        rows = [
            (None, 1.0, 5.0, "2026-10-01"),
            (None, 1.0, 7.0, "2026-10-01"),  # NULL == NULL on the key, payload differs
            (None, 2.0, 5.0, "2026-10-01"),  # a different key
        ]
        result = dedup(spark_session.createDataFrame(rows, self.SCHEMA), ["event_name", "latitude"])

        assert result.count() == 2
