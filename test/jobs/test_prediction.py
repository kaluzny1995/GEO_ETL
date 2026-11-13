import pytest
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
