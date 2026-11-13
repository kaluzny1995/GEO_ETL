"""Per-metric PySpark MLlib training, selection, persistence and prediction."""

from __future__ import annotations

import datetime as dt
import json
import logging
import math
import re
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Final

from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import PolynomialExpansion, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from model.config.datasets import PredictionConfig

logger = logging.getLogger(__name__)

FEATURE_VERSION: Final = 1
LINEAR_MODEL: Final = "polynomial_linear_regression"
GBT_MODEL: Final = "gbt_regressor"
BASE_FEATURES: Final = ("day_index", "dow_sin", "dow_cos", "doy_sin", "doy_cos")


@dataclass(frozen=True)
class CandidateModel:
    """One fitted candidate and its hold-out quality."""

    name: str
    pipeline: Pipeline
    model: PipelineModel
    rmse: float
    mae: float


@dataclass(frozen=True)
class PredictionResult:
    """Predictions for one metric group plus the selected model metadata."""

    predictions: DataFrame
    model_name: str
    training_observations: int
    validation_rmse: float | None
    retrained: bool


def model_key(event_category: str, metric_name: str, metric_unit: str | None) -> str:
    """Return a filesystem-safe model directory name for one metric group."""
    raw = "_".join(part for part in (event_category, metric_name, metric_unit or "") if part)
    safe = re.sub(r"[^a-zA-Z0-9_.-]+", "_", raw).strip("._").lower()
    return safe[:120] or "metric"


def _feature_frame(frame: DataFrame, base_date: dt.date) -> DataFrame:
    """Add trend and cyclical calendar features used by both candidate models."""
    day_index = F.datediff(F.col("file_date"), F.lit(base_date)).cast("double")
    day_of_week = F.dayofweek(F.col("file_date"))
    day_of_year = F.dayofyear(F.col("file_date"))
    return (
        frame.withColumn("day_index", day_index)
        .withColumn("dow_sin", F.sin(F.lit(2 * math.pi) * (day_of_week - 1) / 7))
        .withColumn("dow_cos", F.cos(F.lit(2 * math.pi) * (day_of_week - 1) / 7))
        .withColumn("doy_sin", F.sin(F.lit(2 * math.pi) * (day_of_year - 1) / 365.25))
        .withColumn("doy_cos", F.cos(F.lit(2 * math.pi) * (day_of_year - 1) / 365.25))
    )


def _split_dates(
    frame: DataFrame,
    test_fraction: float,
    count: int | None = None,
) -> tuple[DataFrame, DataFrame, int, int]:
    """Split one metric frame chronologically and return both row counts.

    Args:
        frame: One row per observed date.
        test_fraction: Latest fraction held out for evaluation.
        count: Known row count; avoids an extra Spark action when supplied.

    Returns:
        ``(train, test, train_count, test_count)``.
    """
    if count is None:
        count = frame.count()
    test_count = min(max(1, round(count * test_fraction)), max(1, count - 2))
    train_count = count - test_count
    window = Window.orderBy("file_date")
    numbered = frame.withColumn("_row_number", F.row_number().over(window))
    train = numbered.filter(F.col("_row_number") <= train_count).drop("_row_number")
    test = numbered.filter(F.col("_row_number") > train_count).drop("_row_number")
    return train, test, train_count, test_count


def _linear_pipeline(config: PredictionConfig) -> Pipeline:
    """Polynomial linear-regression candidate."""
    return Pipeline(
        stages=[
            VectorAssembler(inputCols=list(BASE_FEATURES), outputCol="features_raw"),
            PolynomialExpansion(
                degree=config.polynomial_degree,
                inputCol="features_raw",
                outputCol="features",
            ),
            LinearRegression(
                featuresCol="features",
                labelCol="label",
                regParam=config.linear_reg_param,
                maxIter=100,
                elasticNetParam=0.0,
            ),
        ]
    )


def _gbt_pipeline(config: PredictionConfig) -> Pipeline:
    """Gradient-boosted tree candidate for non-linear patterns."""
    return Pipeline(
        stages=[
            VectorAssembler(inputCols=list(BASE_FEATURES), outputCol="features"),
            GBTRegressor(
                featuresCol="features",
                labelCol="label",
                maxIter=config.gbt_max_iter,
                maxDepth=config.gbt_max_depth,
                stepSize=config.gbt_step_size,
                seed=config.random_seed,
            ),
        ]
    )


def _evaluate(model: PipelineModel, frame: DataFrame) -> tuple[float, float]:
    """Return (RMSE, MAE) for a fitted model on *frame*."""
    predictions = model.transform(frame)
    rmse = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="rmse").evaluate(predictions)
    mae = RegressionEvaluator(labelCol="label", predictionCol="prediction", metricName="mae").evaluate(predictions)
    return float(rmse), float(mae)


def _fit_candidates(
    train: DataFrame,
    test: DataFrame,
    config: PredictionConfig,
) -> list[CandidateModel]:
    """Fit both candidates and score them on the chronological hold-out."""
    candidates: list[CandidateModel] = []
    for name, pipeline in (
        (LINEAR_MODEL, _linear_pipeline(config)),
        (GBT_MODEL, _gbt_pipeline(config)),
    ):
        try:
            model = pipeline.fit(train)
            rmse, mae = _evaluate(model, test)
        except Exception as exc:  # one candidate must not prevent the other from training
            logger.warning("Candidate %s failed: %s", name, exc)
            continue
        candidates.append(CandidateModel(name=name, pipeline=pipeline, model=model, rmse=rmse, mae=mae))
    return candidates


def _choose(candidates: list[CandidateModel]) -> CandidateModel:
    """Choose the lowest-RMSE candidate; prefer the extrapolating linear model on ties."""
    return min(candidates, key=lambda candidate: (candidate.rmse, candidate.name != LINEAR_MODEL))


def _model_paths(model_dir: Path, key: str) -> tuple[Path, Path]:
    """Return (Spark model directory, metadata JSON path)."""
    metric_dir = model_dir / key
    return metric_dir / "model", metric_dir / "metadata.json"


def _read_metadata(path: Path) -> dict[str, object] | None:
    """Read persisted model metadata, or return None when it is absent/invalid."""
    if not path.is_file():
        return None
    try:
        metadata = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(metadata, dict) or metadata.get("feature_version") != FEATURE_VERSION:
        return None
    return metadata


def _load_existing_model(
    model_dir: Path,
    key: str,
) -> tuple[PipelineModel, dict[str, object]] | None:
    """Load the persisted selected model and metadata, if present."""
    model_path, metadata_path = _model_paths(model_dir, key)
    metadata = _read_metadata(metadata_path)
    if metadata is None or not model_path.is_dir():
        return None
    try:
        return PipelineModel.load(str(model_path)), metadata
    except Exception as exc:
        logger.warning("Could not load persisted model %s: %s", model_path, exc)
        return None


def _persist_model(
    chosen: CandidateModel,
    full_frame: DataFrame,
    model_dir: Path,
    key: str,
    *,
    event_category: str,
    metric_name: str,
    metric_unit: str | None,
    training_observations: int,
    candidates: list[CandidateModel],
) -> tuple[PipelineModel, float]:
    """Refit the selected pipeline on all observed data and persist it with metadata."""
    final_model = chosen.pipeline.fit(full_frame)
    model_path, metadata_path = _model_paths(model_dir, key)
    model_path.parent.mkdir(parents=True, exist_ok=True)
    # Spark writes the model files from executor processes, which use a different
    # UID than the driver in the Compose stack. The metric directory the driver
    # just created must be writable by those executors as well.
    try:
        model_path.parent.chmod(0o777)
    except PermissionError:
        logger.debug("Could not relax permissions on %s (already writable)", model_path.parent)
    shutil.rmtree(model_path, ignore_errors=True)
    final_model.write().overwrite().save(str(model_path))
    metadata = {
        "feature_version": FEATURE_VERSION,
        "model_name": chosen.name,
        "event_category": event_category,
        "metric_name": metric_name,
        "metric_unit": metric_unit,
        "training_observations": training_observations,
        "validation_rmse": chosen.rmse,
        "validation_mae": chosen.mae,
        "candidate_rmse": {candidate.name: candidate.rmse for candidate in candidates},
        "candidate_mae": {candidate.name: candidate.mae for candidate in candidates},
        "trained_at": dt.datetime.now().isoformat(timespec="seconds"),
    }
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    return final_model, chosen.rmse


def _should_retrain(
    current_rmse: float,
    stored_rmse: float | None,
    metadata: dict[str, object],
    config: PredictionConfig,
) -> bool:
    """Return whether a persisted model's current error is outside tolerance."""
    if config.force_retrain:
        return True
    if stored_rmse is None:
        return True
    if metadata.get("model_name") not in (LINEAR_MODEL, GBT_MODEL):
        return True
    threshold = max(
        float(stored_rmse) * (1.0 + config.retrain_rmse_relative_tolerance),
        float(stored_rmse) + config.retrain_rmse_absolute_tolerance,
    )
    return current_rmse > threshold


def _prediction_value(metric_name: str, config: PredictionConfig):
    """Clamp and round a raw prediction according to metric semantics."""
    prediction = F.col("prediction").cast("double")
    minimum = config.metric_minimums.get(metric_name)
    maximum = config.metric_maximums.get(metric_name)
    if minimum is not None:
        prediction = F.greatest(prediction, F.lit(float(minimum)))
    if maximum is not None:
        prediction = F.least(prediction, F.lit(float(maximum)))
    if metric_name in config.integer_metrics:
        prediction = F.round(prediction).cast("double")
    return prediction


def predict_metric(
    daily: DataFrame,
    missing_dates: DataFrame,
    *,
    event_category: str,
    metric_name: str,
    metric_unit: str | None,
    base_date: dt.date,
    config: PredictionConfig,
    model_dir: Path,
    observed_count: int | None = None,
    missing_count: int | None = None,
) -> PredictionResult | None:
    """Train/reuse one metric model and predict its missing dates.

    Args:
        daily: Observed daily mean rows with ``file_date`` and ``value``.
        missing_dates: Missing dates with a ``file_date`` column.
        event_category: Phenomenon category.
        metric_name: Metric being predicted.
        metric_unit: Metric unit or None.
        base_date: First date of the category range used for trend features.
        config: Prediction configuration.
        model_dir: Root folder for persisted Spark models.
        observed_count: Known number of observed daily rows; avoids a Spark
            action when the caller already aggregated it.
        missing_count: Known number of missing dates; avoids a Spark action when
            the caller already counted them.

    Returns:
        Prediction rows and model metadata, or None when the metric is too
        sparse to train or has no missing dates.
    """
    if observed_count is None:
        observed_count = daily.count()
    if missing_count is None:
        missing_count = missing_dates.count()
    if observed_count < config.min_observations or missing_count == 0:
        return None

    featured = _feature_frame(daily, base_date).withColumn("label", F.col("value").cast("double"))
    train, test, train_count, test_count = _split_dates(featured, config.test_fraction, observed_count)
    if train_count < 2 or test_count < 1:
        return None

    key = model_key(event_category, metric_name, metric_unit)
    existing = _load_existing_model(model_dir, key)
    chosen_model: PipelineModel | None = None
    model_name: str | None = None
    validation_rmse: float | None = None
    retrained = False

    if existing is not None:
        model, metadata = existing
        stored_rmse = metadata.get("validation_rmse")
        try:
            current_rmse, _ = _evaluate(model, test)
        except Exception as exc:
            logger.warning("Could not evaluate persisted model %s: %s", key, exc)
            current_rmse = float("inf")
        if not _should_retrain(
            current_rmse,
            float(stored_rmse) if isinstance(stored_rmse, (int, float)) else None,
            metadata,
            config,
        ):
            chosen_model = model
            model_name = str(metadata["model_name"])
            validation_rmse = float(stored_rmse) if isinstance(stored_rmse, (int, float)) else None

    if chosen_model is None:
        candidates = _fit_candidates(train, test, config)
        if not candidates:
            logger.warning("No MLlib candidate could be trained for %s", key)
            return None
        chosen = _choose(candidates)
        chosen_model, validation_rmse = _persist_model(
            chosen,
            featured,
            model_dir,
            key,
            event_category=event_category,
            metric_name=metric_name,
            metric_unit=metric_unit,
            training_observations=observed_count,
            candidates=candidates,
        )
        model_name = chosen.name
        retrained = True

    missing_features = _feature_frame(missing_dates, base_date)
    scored = chosen_model.transform(missing_features)
    prediction_value = _prediction_value(metric_name, config)
    predicted_at = dt.datetime.now()
    if model_name is None:  # defensive: both the reuse and retrain paths set it
        raise RuntimeError(f"Model name missing for {key}")
    predictions = scored.select(
        F.lit(event_category).alias("event_category"),
        F.lit(metric_name).alias("metric_name"),
        F.lit(metric_unit).cast("string").alias("metric_unit"),
        F.col("file_date").cast("date").alias("file_date"),
        prediction_value.alias("predicted_value"),
        F.lit(True).alias("is_predicted"),
        F.lit(model_name).alias("model_name"),
        F.lit(observed_count).cast("int").alias("training_observations"),
        F.lit(validation_rmse).cast("double").alias("validation_rmse"),
        F.lit(predicted_at).cast("timestamp").alias("predicted_at"),
    )
    return PredictionResult(
        predictions=predictions,
        model_name=model_name,
        training_observations=observed_count,
        validation_rmse=validation_rmse,
        retrained=retrained,
    )
