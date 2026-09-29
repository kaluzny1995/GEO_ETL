"""Data quality metrics computed per ``file_date`` with PySpark.

Every metric is a plain aggregate, so a single
``groupBy("file_date").agg(...)`` pass computes the whole metric set for a
table.  The wide frame is then unpivoted to the long layout of
:class:`model.other.data_quality.DataQuality` and appended to
``geo.data_quality``.

Metrics produced per ``file_date``:

* ``total_records`` — number of rows.
* ``null_pct__<column>`` — share of rows where the column is NULL (one per column).
* ``bad_pct__<rule>`` — share of rows violating a rule (one per rule).
* ``bad_row_pct`` — share of rows violating *any* rule.
* ``dup_row_pct`` — share of full-row duplicates inside the file_date.
* ``unique_row_pct`` — share of distinct rows inside the file_date.
* ``min__<column>``, ``max__<column>``, ``range__<column>`` — observed value
  range of a numeric column.
* ``mean__<column>``, ``stddev__<column>`` — mean and population standard
  deviation of a numeric column.
* ``p05__<column>`` … ``p95__<column>`` — approximate 5/25/50/75/95th
  percentile of a numeric column (one metric per point).
* ``distinct__<column>`` — number of distinct non-NULL values (one per column,
  ``file_date`` excluded).

A column is *numeric* when its Pydantic annotation is ``int``, ``float`` or
``bool`` (unwrapping ``Optional``); those are the columns that receive the
range/spread/percentile metrics, while ``distinct__*`` covers every column.
A statistic that is undefined for a ``file_date`` — e.g. ``min`` of a column
that is entirely NULL that day — is omitted instead of stored as NULL, because
``geo.data_quality.value`` is NOT NULL.

Nulls are reported by ``null_pct__*`` only — a rule that cannot be evaluated
because of a NULL does not count the row as incorrect, so the two metric
families never double count.
"""

import types
from dataclasses import dataclass
from typing import Any, Union, get_args, get_origin

from pydantic import BaseModel
from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

FILE_DATE_COL = "file_date"
METRICS_TABLE = "data_quality"

TOTAL_RECORDS_METRIC = "total_records"
NULL_METRIC_PREFIX = "null_pct__"
BAD_METRIC_PREFIX = "bad_pct__"
BAD_ROW_METRIC = "bad_row_pct"
DUP_ROW_METRIC = "dup_row_pct"
UNIQUE_ROW_METRIC = "unique_row_pct"
MIN_METRIC_PREFIX = "min__"
MAX_METRIC_PREFIX = "max__"
RANGE_METRIC_PREFIX = "range__"
MEAN_METRIC_PREFIX = "mean__"
STDDEV_METRIC_PREFIX = "stddev__"
DISTINCT_METRIC_PREFIX = "distinct__"
PERCENTILE_POINTS = (5, 25, 50, 75, 95)

NUMERIC_TYPES = (int, float, bool)

GEO_TABLES = frozenset(
    {
        "eonet_event",
        "gdacs_event",
        "nifc_fire",
        "openmeteo_flood",
        "usgs_earthquake",
        "usgs_nwis",
    }
)


@dataclass(frozen=True)
class Rule:
    """A value rule: ``cond`` evaluates ``True`` for rows that pass."""

    name: str
    cond: Column


def _base_type(annotation: Any) -> Any:
    """Unwrap ``Optional``/``Union`` annotations to their base type."""
    origin = get_origin(annotation)
    if origin is Union or isinstance(annotation, types.UnionType):
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def model_rules(model: type[BaseModel], columns: list[str]) -> list[Rule]:
    """Build the rules implied by a Pydantic model.

    Required string columns must not be blank, and ``file_date`` must be
    an ISO ``YYYY-MM-DD`` date.  NULLs are already covered by the
    ``null_pct__*`` metrics, so they are not treated as rule violations.

    Args:
        model: Pydantic model of the table being measured.
        columns: Columns actually present in the DataFrame.

    Returns:
        One :class:`Rule` per applicable model constraint.
    """
    column_set = set(columns)
    rules = []
    for name, field_info in model.model_fields.items():
        if name == FILE_DATE_COL or name not in column_set:
            continue
        if _base_type(field_info.annotation) is str and field_info.is_required():
            rules.append(Rule(f"not_blank__{name}", F.trim(F.col(name)) != F.lit("")))
    if FILE_DATE_COL in column_set:
        rules.append(Rule("file_date_iso", F.col(FILE_DATE_COL).rlike(r"^\d{4}-\d{2}-\d{2}$")))
    return rules


def numeric_columns(model: type[BaseModel], columns: list[str]) -> list[str]:
    """Numeric columns of *model* that are present in the DataFrame.

    A column is numeric when its annotation unwraps to ``int``, ``float`` or
    ``bool`` — those are the columns that receive the range, spread and
    percentile metrics.  ``file_date`` is the grouping key and columns absent
    from the DataFrame are skipped, so the metric set always matches the
    actual table.

    Args:
        model: Pydantic model of the table being measured.
        columns: Columns actually present in the DataFrame.

    Returns:
        Numeric column names in model field order.
    """
    column_set = set(columns)
    return [
        name
        for name, field_info in model.model_fields.items()
        if name != FILE_DATE_COL and name in column_set and _base_type(field_info.annotation) in NUMERIC_TYPES
    ]


def dataset_rules(table_name: str, columns: list[str]) -> list[Rule]:
    """Build dataset-specific value rules for *table_name*.

    Rules whose column is missing from *columns* are skipped, so the
    metric set always matches the actual table.

    Args:
        table_name: Registered table name (e.g. ``"avalanche_org"``).
        columns: Columns actually present in the DataFrame.

    Returns:
        Zero or more dataset-specific :class:`Rule` objects.
    """
    column_set = set(columns)
    rules: list[Rule] = []

    def add(name: str, cond: Column, *required: str) -> None:
        if all(column in column_set for column in required):
            rules.append(Rule(name, cond))

    if table_name in GEO_TABLES:
        add("latitude_range", F.col("latitude").between(-90, 90), "latitude")
        add("longitude_range", F.col("longitude").between(-180, 180), "longitude")

    if table_name == "avalanche_org":
        add(
            "danger_level_domain",
            F.col("danger_level").between(1, 5) | (F.col("danger_level") == -1),
            "danger_level",
        )
        add(
            "forecast_order",
            (F.col("forecast_end") >= F.col("forecast_start"))
            | F.col("forecast_end").isNull()
            | F.col("forecast_start").isNull(),
            "forecast_start",
            "forecast_end",
        )
    elif table_name == "eonet_event":
        add("geometry_count_non_negative", F.col("geometry_count") >= 0, "geometry_count")
    elif table_name == "gdacs_event":
        add("alert_level_domain", F.col("alert_level").isin("Green", "Orange", "Red"), "alert_level")
    elif table_name == "ibtracs_cyclone":
        add("latitude_range", F.col("lat").between(-90, 90), "lat")
        add("longitude_range", F.col("lon").between(-180, 180), "lon")
        add("wmo_wind_non_negative", F.col("wmo_wind") >= 0, "wmo_wind")
    elif table_name == "nifc_fire":
        add("acres_non_negative", F.col("acres") >= 0, "acres")
        add("percent_contained_0_100", F.col("percent_contained").between(0, 100), "percent_contained")
    elif table_name == "openmeteo_flood":
        add("discharge_non_negative", F.col("discharge_m3s") >= 0, "discharge_m3s")

    return rules


def rules_for(table_name: str, model: type[BaseModel], columns: list[str]) -> list[Rule]:
    """Return model rules followed by dataset-specific rules for a table.

    Args:
        table_name: Registered table name.
        model: Pydantic model of the table being measured.
        columns: Columns actually present in the DataFrame.

    Returns:
        The combined list of :class:`Rule` objects.
    """
    return model_rules(model, columns) + dataset_rules(table_name, columns)


def compute_metrics(df: DataFrame, *, table_name: str, model: type[BaseModel]) -> DataFrame:
    """Compute all quality metrics for *table_name*, one row per file_date.

    Args:
        df: DataFrame of the source table.
        table_name: Registered table name (selects the dataset rules).
        model: Pydantic model of the source table.

    Returns:
        Wide frame: ``file_date`` plus one column per metric.

    Raises:
        ValueError: If the table has no ``file_date`` column.
    """
    if FILE_DATE_COL not in df.columns:
        raise ValueError(f"Table '{table_name}' has no '{FILE_DATE_COL}' column — metrics cannot be grouped")

    rules = rules_for(table_name, model, df.columns)
    row_hash = F.xxhash64(*[F.col(column) for column in df.columns])
    total = F.count(F.lit(1))
    distinct_rows = F.countDistinct(row_hash)

    any_failed: Column | None = None
    for rule in rules:
        failed = F.coalesce(~rule.cond, F.lit(False))
        any_failed = failed if any_failed is None else (any_failed | failed)

    aggs: list[Column] = [total.alias(TOTAL_RECORDS_METRIC)]
    aggs += [
        F.avg(F.coalesce(F.col(column).isNull(), F.lit(False)).cast("int")).alias(f"{NULL_METRIC_PREFIX}{column}")
        for column in df.columns
    ]
    aggs += [
        F.avg(F.coalesce(~rule.cond, F.lit(False)).cast("int")).alias(f"{BAD_METRIC_PREFIX}{rule.name}")
        for rule in rules
    ]
    if any_failed is not None:
        aggs.append(F.avg(F.coalesce(any_failed, F.lit(False)).cast("int")).alias(BAD_ROW_METRIC))
    aggs += [
        ((total - distinct_rows) / total).alias(DUP_ROW_METRIC),
        (distinct_rows / total).alias(UNIQUE_ROW_METRIC),
    ]

    for column in numeric_columns(model, df.columns):
        value = F.col(column).cast("double")
        aggs += [
            F.min(value).alias(f"{MIN_METRIC_PREFIX}{column}"),
            F.max(value).alias(f"{MAX_METRIC_PREFIX}{column}"),
            (F.max(value) - F.min(value)).alias(f"{RANGE_METRIC_PREFIX}{column}"),
            F.avg(value).alias(f"{MEAN_METRIC_PREFIX}{column}"),
            F.stddev_pop(value).alias(f"{STDDEV_METRIC_PREFIX}{column}"),
        ]
        aggs += [
            F.percentile_approx(value, point / 100).alias(f"p{point:02d}__{column}") for point in PERCENTILE_POINTS
        ]
    aggs += [
        F.countDistinct(F.col(column)).alias(f"{DISTINCT_METRIC_PREFIX}{column}")
        for column in df.columns
        if column != FILE_DATE_COL
    ]
    return df.groupBy(FILE_DATE_COL).agg(*aggs)


def to_metrics_rows(wide: DataFrame, *, table_name: str) -> DataFrame:
    """Unpivot a wide metric frame to the long ``DataQuality`` layout.

    Metric values that are NULL — a statistic undefined for a ``file_date``,
    such as ``min`` of a column that is entirely NULL that day — are dropped:
    ``geo.data_quality.value`` is NOT NULL.

    Args:
        wide: Output of :func:`compute_metrics`.
        table_name: Value stored in the ``table_name`` column.

    Returns:
        Frame with ``table_name, file_date, metric, value, computed_at``.
    """
    metric_columns = [column for column in wide.columns if column != FILE_DATE_COL]
    stacked = F.explode(
        F.array(
            *[
                F.struct(F.lit(column).alias("metric"), F.col(column).cast("double").alias("value"))
                for column in metric_columns
            ]
        )
    )
    return (
        wide.select(F.lit(table_name).alias("table_name"), F.col(FILE_DATE_COL), stacked.alias("_metric"))
        .select(
            F.col("table_name"),
            F.col(FILE_DATE_COL),
            F.col("_metric.metric").alias("metric"),
            F.col("_metric.value").alias("value"),
            F.current_timestamp().alias("computed_at"),
        )
        .where(F.col("value").isNotNull())
    )


def data_quality_rows(df: DataFrame, *, table_name: str, model: type[BaseModel]) -> DataFrame:
    """Compute quality metrics for *df* in the ``DataQuality`` row layout.

    Args:
        df: DataFrame of the source table.
        table_name: Registered table name.
        model: Pydantic model of the source table.

    Returns:
        One row per ``(table_name, file_date, metric)``.
    """
    wide = compute_metrics(df, table_name=table_name, model=model)
    return to_metrics_rows(wide, table_name=table_name)
