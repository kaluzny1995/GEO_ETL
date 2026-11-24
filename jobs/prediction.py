"""PREDICTION job: train/reuse per-metric MLlib models and rebuild ``prediction_etl``."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ml.predictor import PredictionResult, predict_metric
from model.config import AppConfig, SupabaseConnectionConfig
from model.config.datasets import PredictionConfig, load_prediction_config
from model.other.prediction_etl import PredictionEtl
from utils.ddl_generator import execute_ddl, execute_statement, pydantic_to_ddl

logger = logging.getLogger(__name__)

_cfg: PredictionConfig = load_prediction_config()


def _metric_filter(frame: DataFrame) -> DataFrame:
    """Remove excluded metrics and normalise the source columns."""
    return (
        frame.filter(~F.col("metric_name").isin(_cfg.exclude_metrics))
        .withColumn("file_date", F.to_date("file_date"))
        .withColumn("metric_value", F.col("metric_value").cast("double"))
        .filter(F.col("metric_value").isNotNull() & F.col("is_predicted").eqNullSafe(F.lit(False)))
    )


def _group_filter(daily: DataFrame, category: str, metric_name: str, metric_unit: str | None) -> DataFrame:
    """Filter one metric group, including a null-safe metric-unit match."""
    frame = daily.filter((F.col("event_category") == category) & (F.col("metric_name") == metric_name))
    if metric_unit is None:
        return frame.filter(F.col("metric_unit").isNull())
    return frame.filter(F.col("metric_unit") == metric_unit)


def _delete_existing(spark: SparkSession, schema: str, jdbc_url: str, properties: dict[str, str], table: str) -> None:
    """Remove the previous prediction run so reruns are idempotent."""
    execute_statement(spark, f"DELETE FROM {schema}.{table}", jdbc_url, properties)


def _stack_predictions(accumulated: DataFrame | None, new: DataFrame) -> DataFrame:
    """Append one metric group's predictions to the running union.

    Args:
        accumulated: Predictions of the groups processed so far, or ``None``
            for the first group.
        new: Predictions of the next group (same columns).

    Returns:
        The stacked predictions. The union is lazy; :func:`_persisted` is what
        materializes it once before counting and writing.
    """
    return new if accumulated is None else accumulated.unionByName(new)


@contextmanager
def _persisted(frame: DataFrame) -> Iterator[DataFrame]:
    """Materialize *frame* once for the count + write, then release the cache.

    Building the predictions as one union per metric group creates a long
    lineage of MLlib transforms. Without a cache, ``count()`` and the JDBC
    write recompute every group and keep all intermediate plans alive at once.
    MEMORY_AND_DISK spills to executor disk instead of failing a driver with a
    small heap; the ``finally`` always releases the blocks, even when the write
    raises.
    """
    cached = frame.persist(StorageLevel.MEMORY_AND_DISK)
    try:
        yield cached
    finally:
        cached.unpersist()


def run(spark: SparkSession, table_name: str, logger: logging.Logger | None = None) -> str:
    """Train/reuse models and rebuild every missing predicted metric/date row.

    Args:
        spark: Active Spark session.
        table_name: Prediction table name from the CLI; must match the configured table.
        logger: Optional job logger.

    Returns:
        A human-readable completion summary.

    Raises:
        ValueError: When the requested table does not match the prediction configuration.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    if table_name != _cfg.table_name:
        raise ValueError(f"PREDICTION writes only {_cfg.table_name!r}, got {table_name!r}")

    app_config = AppConfig.from_json()
    db_config = SupabaseConnectionConfig.from_env()
    schema = app_config.database.db_schema
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(app_config.database.jdbc_driver)

    model_dir = Path(__file__).resolve().parents[1] / _cfg.model_directory
    model_dir.mkdir(parents=True, exist_ok=True)
    # Spark model writers run on the executors, which use a different UID than the
    # driver in the Compose stack. Keep the shared model root writable by all of
    # them; the prediction artifacts are not sensitive.
    model_dir.chmod(0o777)
    logger.info("Prediction model directory: %s", model_dir)

    execute_ddl(spark, pydantic_to_ddl(PredictionEtl, table_name, schema=schema), jdbc_url, properties)
    logger.info("Prediction table ready: %s.%s", schema, table_name)

    observed = spark.read.jdbc(url=jdbc_url, table=_cfg.source_view, properties=properties)
    observed = _metric_filter(observed)
    daily = observed.groupBy("event_category", "metric_name", "metric_unit", "file_date").agg(
        F.avg("metric_value").alias("value")
    )
    daily.cache()
    try:
        bounds = daily.groupBy("event_category").agg(
            F.min("file_date").alias("min_date"),
            F.max("file_date").alias("max_date"),
        )
        category_bounds = {row.event_category: (row.min_date, row.max_date) for row in bounds.collect()}
        # Expand each category's full date range inside Spark (JVM only): creating a
        # local Python list of dates would force a Python-worker stage, which breaks
        # when the driver runs outside the Spark worker image.
        date_ranges = bounds.select(
            F.col("event_category"),
            F.explode(
                F.sequence(
                    F.col("min_date"),
                    F.col("max_date"),
                    F.expr("interval 1 day"),
                )
            ).alias("file_date"),
        )
        group_counts = {
            (row.event_category, row.metric_name, row.metric_unit): row["count"]
            for row in daily.groupBy("event_category", "metric_name", "metric_unit").count().collect()
        }
        logger.info("Training/predicting %d metric groups from %s", len(group_counts), _cfg.source_view)

        predictions: DataFrame | None = None
        retrained = 0
        skipped = 0
        for (category, metric_name, metric_unit), observed_count in group_counts.items():
            if observed_count < _cfg.min_observations:
                skipped += 1
                logger.info("Skipping sparse metric %s/%s/%s", category, metric_name, metric_unit)
                continue

            group_daily = _group_filter(daily, category, metric_name, metric_unit)
            start, _end = category_bounds[category]
            missing_df = (
                date_ranges.filter(F.col("event_category") == category)
                .join(group_daily.select("file_date"), on="file_date", how="left_anti")
                .select("file_date")
            )
            missing_count = missing_df.count()
            if missing_count == 0:
                continue
            result: PredictionResult | None = predict_metric(
                group_daily,
                missing_df,
                event_category=category,
                metric_name=metric_name,
                metric_unit=metric_unit,
                base_date=start,
                config=_cfg,
                model_dir=model_dir,
                observed_count=observed_count,
                missing_count=missing_count,
            )
            if result is None:
                skipped += 1
                continue
            if result.retrained:
                retrained += 1
            logger.info(
                "%s/%s/%s: %s predicted rows using %s (%s)",
                category,
                metric_name,
                metric_unit,
                missing_count,
                result.model_name,
                "retrained" if result.retrained else "reused",
            )
            predictions = _stack_predictions(predictions, result.predictions)

        _delete_existing(spark, schema, jdbc_url, properties, table_name)
        if predictions is None:
            logger.warning("No metric group had enough observations to predict")
            return f"No predictions written to {schema}.{table_name} — all metric groups were skipped"

        with _persisted(predictions) as materialized:
            row_count = materialized.count()
            logger.info("Writing %d predicted rows to %s.%s", row_count, schema, table_name)
            materialized.write.jdbc(url=jdbc_url, table=f"{schema}.{table_name}", mode="append", properties=properties)
        summary = (
            f"Wrote {row_count} predictions to {schema}.{table_name} "
            f"({len(group_counts)} groups, {retrained} retrained, {skipped} skipped)"
        )
        logger.info(summary)
        return summary
    finally:
        daily.unpersist()
