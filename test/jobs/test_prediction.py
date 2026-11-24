import pytest
from pyspark import StorageLevel
from pyspark.sql import functions as F

from jobs import prediction


class TestMetricFilter:
    def test_excludes_danger_metrics_but_keeps_volcano_numbers(self, spark_session):
        frame = spark_session.createDataFrame(
            [
                ("avalanche", "size", "class", 2.0, "2026-11-01", False),
                ("avalanche", "danger_level", "NAPADS", 3.0, "2026-11-01", False),
                ("volcano", "vnum", "Smithsonian", 123.0, "2026-11-01", False),
                ("volcano", "vei", "VEI", 2.0, "2026-11-01", False),
                ("volcano", "vei", "VEI", 9.0, "2026-11-02", True),
            ],
            ["event_category", "metric_name", "metric_unit", "metric_value", "file_date", "is_predicted"],
        )

        filtered = prediction._metric_filter(frame)

        assert {row.metric_name for row in filtered.collect()} == {"size", "vei", "vnum"}
        assert filtered.filter(F.col("is_predicted")).count() == 0

    def test_group_filter_matches_null_units(self, spark_session):
        frame = spark_session.createDataFrame(
            [
                ("flood", "magnitude", None, 1.0),
                ("flood", "discharge", "m3/s", 2.0),
            ],
            ["event_category", "metric_name", "metric_unit", "value"],
        )

        assert prediction._group_filter(frame, "flood", "magnitude", None).count() == 1
        assert prediction._group_filter(frame, "flood", "discharge", "m3/s").count() == 1


class TestRunContract:
    def test_rejects_unexpected_table_name(self):
        with pytest.raises(ValueError, match="prediction_etl"):
            prediction.run(object(), "other_table")  # type: ignore[arg-type]


class TestPredictionMaterialization:
    """Tests for the helpers that keep the multi-group prediction plan bounded."""

    def test_stack_predictions_starts_with_the_first_group(self, spark_session):
        """The first group is returned as-is; later groups are unioned by name."""
        first = spark_session.createDataFrame([("avalanche", 1.0)], ["event_category", "predicted_value"])
        second = spark_session.createDataFrame([("fire", 2.0)], ["event_category", "predicted_value"])

        assert prediction._stack_predictions(None, first).count() == 1
        stacked = prediction._stack_predictions(first, second)
        assert stacked.count() == 2
        assert {row.event_category for row in stacked.collect()} == {"avalanche", "fire"}

    def test_persisted_materializes_with_memory_and_disk_and_releases_it(self):
        """The union is cached once for count + write and always unpersisted."""
        from unittest.mock import MagicMock

        frame = MagicMock()
        materialized = MagicMock()
        frame.persist.return_value = materialized

        with prediction._persisted(frame) as cached:
            assert cached is materialized
            frame.persist.assert_called_once_with(StorageLevel.MEMORY_AND_DISK)

        materialized.unpersist.assert_called_once_with()

    def test_persisted_unpersists_when_the_write_fails(self):
        """A failed JDBC write must not leave the prediction blocks cached."""
        from unittest.mock import MagicMock

        frame = MagicMock()
        materialized = MagicMock()
        frame.persist.return_value = materialized

        with pytest.raises(RuntimeError, match="write failed"), prediction._persisted(frame):
            raise RuntimeError("write failed")

        materialized.unpersist.assert_called_once_with()
