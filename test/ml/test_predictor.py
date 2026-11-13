import datetime as dt
import json

from pyspark.sql import functions as F

from ml.predictor import GBT_MODEL, LINEAR_MODEL, model_key, predict_metric
from model.config.datasets import PredictionConfig


def _daily(spark, *, count: int = 20, missing: set[int] | None = None, offset: float = 0.0):
    missing = missing or set()
    start = dt.date(2026, 9, 1)
    rows = [
        (start + dt.timedelta(days=index), 10.0 + index * 0.7 + (index % 5) + offset)
        for index in range(count)
        if index not in missing
    ]
    return start, spark.createDataFrame(rows, ["file_date", "value"])


def _missing(spark, start: dt.date, indexes):
    return spark.createDataFrame([(start + dt.timedelta(days=index),) for index in indexes], ["file_date"])


def _config() -> PredictionConfig:
    return PredictionConfig(
        min_observations=5,
        test_fraction=0.2,
        retrain_rmse_relative_tolerance=0.1,
        retrain_rmse_absolute_tolerance=0.0,
        exclude_metrics=[],
        integer_metrics=[],
        metric_minimums={"size": 0},
        metric_maximums={},
        model_directory="ml_models",
    )


class TestModelKey:
    def test_is_filesystem_safe(self):
        assert model_key("avalanche", "size", "destructive size class") == ("avalanche_size_destructive_size_class")

    def test_handles_missing_unit(self):
        assert model_key("flood", "discharge", None) == "flood_discharge"


class TestPredictMetric:
    def test_trains_selects_and_persists_model(self, spark_session, tmp_path):
        start, daily = _daily(spark_session, missing={3, 7, 8, 15})
        missing = _missing(spark_session, start, {3, 7, 8, 15})

        result = predict_metric(
            daily,
            missing,
            event_category="avalanche",
            metric_name="size",
            metric_unit="class",
            base_date=start,
            config=_config(),
            model_dir=tmp_path,
        )

        assert result is not None
        assert result.retrained is True
        assert result.model_name in (LINEAR_MODEL, GBT_MODEL)
        assert result.validation_rmse is not None
        assert result.predictions.count() == 4
        assert result.predictions.filter(F.col("is_predicted").eqNullSafe(F.lit(True))).count() == 4
        metadata = json.loads((tmp_path / "avalanche_size_class" / "metadata.json").read_text())
        assert metadata["model_name"] == result.model_name
        assert metadata["training_observations"] == 16
        assert (tmp_path / "avalanche_size_class" / "model").is_dir()

    def test_creates_a_missing_model_directory(self, spark_session, tmp_path):
        """A fresh checkout has no models: the first run creates the folder and trains."""
        start, daily = _daily(spark_session, missing={3, 7})
        missing = _missing(spark_session, start, {3, 7})
        model_dir = tmp_path / "nested" / "ml_models"

        assert not model_dir.exists()
        result = predict_metric(
            daily,
            missing,
            event_category="avalanche",
            metric_name="size",
            metric_unit="class",
            base_date=start,
            config=_config(),
            model_dir=model_dir,
        )

        assert result is not None
        assert result.retrained is True
        model_path = model_dir / "avalanche_size_class"
        assert (model_path / "metadata.json").is_file()
        assert (model_path / "model").is_dir()
        # Spark writes from executor UIDs; the metric directory must be shared.
        assert model_path.stat().st_mode & 0o777 == 0o777

    def test_reuses_persisted_model_within_tolerance(self, spark_session, tmp_path):
        start, daily = _daily(spark_session, missing={3, 7})
        missing = _missing(spark_session, start, {3, 7})
        kwargs = {
            "event_category": "avalanche",
            "metric_name": "size",
            "metric_unit": "class",
            "base_date": start,
            "config": _config(),
            "model_dir": tmp_path,
        }

        first = predict_metric(daily, missing, **kwargs)
        second = predict_metric(daily, missing, **kwargs)

        assert first is not None and first.retrained is True
        assert second is not None and second.retrained is False
        assert second.model_name == first.model_name

    def test_retrains_when_error_breaks_tolerance(self, spark_session, tmp_path):
        start, daily = _daily(spark_session, missing={3, 7})
        missing = _missing(spark_session, start, {3, 7})
        kwargs = {
            "event_category": "avalanche",
            "metric_name": "size",
            "metric_unit": "class",
            "base_date": start,
            "config": _config(),
            "model_dir": tmp_path,
        }

        first = predict_metric(daily, missing, **kwargs)
        _, shifted = _daily(spark_session, missing={3, 7}, offset=1000.0)
        drifted = predict_metric(shifted, missing, **kwargs)

        assert first is not None and first.retrained is True
        assert drifted is not None and drifted.retrained is True

    def test_skips_sparse_metric(self, spark_session, tmp_path):
        start, daily = _daily(spark_session, count=4)
        missing = _missing(spark_session, start, {4})

        result = predict_metric(
            daily,
            missing,
            event_category="avalanche",
            metric_name="size",
            metric_unit="class",
            base_date=start,
            config=_config(),
            model_dir=tmp_path,
        )

        assert result is None
        assert not any(tmp_path.iterdir())

    def test_clamps_and_rounds_predictions(self, spark_session, tmp_path):
        start, daily = _daily(spark_session, missing={3, 7})
        missing = _missing(spark_session, start, {3, 7})
        config = _config().model_copy(
            update={
                "metric_minimums": {"vei": 0.0},
                "metric_maximums": {"vei": 8.0},
                "integer_metrics": ["vei"],
            }
        )

        result = predict_metric(
            daily,
            missing,
            event_category="volcano",
            metric_name="vei",
            metric_unit="VEI",
            base_date=start,
            config=config,
            model_dir=tmp_path,
        )

        assert result is not None
        values = [row.predicted_value for row in result.predictions.collect()]
        assert all(0.0 <= value <= 8.0 for value in values)
        assert all(float(value).is_integer() for value in values)
