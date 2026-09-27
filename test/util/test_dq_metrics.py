import datetime as dt

import pytest
from pydantic import BaseModel
from pyspark.sql import Row

from model.other.data_quality import DataQuality
from model.plain.avalanche_org import AvalancheOrg
from utils.dq_metrics import (
    BAD_METRIC_PREFIX,
    BAD_ROW_METRIC,
    DUP_ROW_METRIC,
    FILE_DATE_COL,
    METRICS_TABLE,
    NULL_METRIC_PREFIX,
    TOTAL_RECORDS_METRIC,
    UNIQUE_ROW_METRIC,
    compute_metrics,
    data_quality_rows,
    dataset_rules,
    model_rules,
    rules_for,
    to_metrics_rows,
)
from utils.schema_converter import pydantic_to_spark_schema

COLUMNS = ["zone_id", "danger_level", "zone_name", "forecast_start", "forecast_end", "file_date"]

ROWS = [
    ("1", "3", "Alpha", "2026-09-01 10:00", "2026-09-01 12:00", "2026-09-01"),
    ("2", "9", "Bravo", "2026-09-01 10:00", "2026-09-01 12:00", "2026-09-01"),
    ("3", "1", "", None, None, "2026-09-01"),
    ("1", "3", "Alpha", "2026-09-01 10:00", "2026-09-01 12:00", "2026-09-01"),
    ("4", "1", "Delta", "2026-09-01 10:00", None, "2026-09-02"),
    ("5", "2", "Echo", None, "2026-09-02 12:00", "2026-09-02"),
]

METRICS_PER_DATE = 14


@pytest.fixture
def source_df(spark_session):
    return spark_session.createDataFrame([Row(**dict(zip(COLUMNS, row, strict=True))) for row in ROWS])


@pytest.fixture
def wide(source_df):
    return compute_metrics(source_df, table_name="avalanche_org", model=AvalancheOrg)


def _value(wide, file_date, metric):
    row = wide.filter(wide[FILE_DATE_COL] == file_date).collect()[0]
    return row[metric]


class TestConstants:
    def test_metric_names(self):
        assert FILE_DATE_COL == "file_date"
        assert METRICS_TABLE == "data_quality"
        assert TOTAL_RECORDS_METRIC == "total_records"
        assert NULL_METRIC_PREFIX == "null_pct__"
        assert BAD_METRIC_PREFIX == "bad_pct__"
        assert BAD_ROW_METRIC == "bad_row_pct"
        assert DUP_ROW_METRIC == "dup_row_pct"
        assert UNIQUE_ROW_METRIC == "unique_row_pct"


class TestRules:
    def test_model_rules_require_active_spark(self, spark_session):
        rules = model_rules(AvalancheOrg, COLUMNS)
        names = [r.name for r in rules]
        assert names == ["not_blank__zone_name", "file_date_iso"]

    def test_model_rules_skip_missing_columns(self, spark_session):
        rules = model_rules(AvalancheOrg, ["zone_id", "file_date"])
        assert [r.name for r in rules] == ["file_date_iso"]

    def test_model_rules_without_file_date_column(self, spark_session):
        rules = model_rules(AvalancheOrg, ["zone_id", "zone_name"])
        assert [r.name for r in rules] == ["not_blank__zone_name"]

    def test_model_rules_skip_optional_string(self, spark_session):
        class OptionalString(BaseModel):
            name: str | None = None
            file_date: str

        rules = model_rules(OptionalString, ["name", "file_date"])
        assert [r.name for r in rules] == ["file_date_iso"]

    def test_dataset_rules_avalanche(self, spark_session):
        rules = dataset_rules("avalanche_org", COLUMNS)
        assert [r.name for r in rules] == ["danger_level_domain", "forecast_order"]

    def test_dataset_rules_skip_missing_columns(self, spark_session):
        assert dataset_rules("avalanche_org", ["zone_id"]) == []

    def test_dataset_rules_geo_tables(self, spark_session):
        rules = dataset_rules("usgs_earthquake", ["latitude", "longitude"])
        assert [r.name for r in rules] == ["latitude_range", "longitude_range"]

    def test_dataset_rules_ibtracs_uses_lat_lon(self, spark_session):
        rules = dataset_rules("ibtracs_cyclone", ["lat", "lon", "wmo_wind"])
        assert [r.name for r in rules] == ["latitude_range", "longitude_range", "wmo_wind_non_negative"]

    def test_rules_for_combines_model_and_dataset(self, spark_session):
        rules = rules_for("avalanche_org", AvalancheOrg, COLUMNS)
        assert [r.name for r in rules] == [
            "not_blank__zone_name",
            "file_date_iso",
            "danger_level_domain",
            "forecast_order",
        ]

    def test_rule_condition_is_column(self, spark_session):
        from pyspark.sql import Column

        for rule in rules_for("avalanche_org", AvalancheOrg, COLUMNS):
            assert isinstance(rule.cond, Column)

    def test_danger_level_domain_accepts_no_rating_sentinel(self, spark_session):
        rule = next(r for r in dataset_rules("avalanche_org", COLUMNS) if r.name == "danger_level_domain")
        df = spark_session.createDataFrame([(-1,), (1,), (5,), (0,), (6,), (None,)], "danger_level int")
        outcomes = {r.danger_level: r.passes for r in df.select("danger_level", rule.cond.alias("passes")).collect()}
        assert outcomes == {-1: True, 1: True, 5: True, 0: False, 6: False, None: None}


class TestComputeMetrics:
    def test_columns(self, wide):
        expected = (
            [FILE_DATE_COL, TOTAL_RECORDS_METRIC]
            + [f"{NULL_METRIC_PREFIX}{c}" for c in COLUMNS]
            + [f"{BAD_METRIC_PREFIX}not_blank__zone_name", f"{BAD_METRIC_PREFIX}file_date_iso"]
            + [f"{BAD_METRIC_PREFIX}danger_level_domain", f"{BAD_METRIC_PREFIX}forecast_order"]
            + [BAD_ROW_METRIC, DUP_ROW_METRIC, UNIQUE_ROW_METRIC]
        )
        assert sorted(wide.columns) == sorted(expected)

    def test_one_row_per_file_date(self, wide):
        assert wide.count() == 2

    def test_total_records(self, wide):
        assert _value(wide, "2026-09-01", TOTAL_RECORDS_METRIC) == 4
        assert _value(wide, "2026-09-02", TOTAL_RECORDS_METRIC) == 2

    def test_null_percentages(self, wide):
        assert _value(wide, "2026-09-01", f"{NULL_METRIC_PREFIX}forecast_start") == 0.25
        assert _value(wide, "2026-09-01", f"{NULL_METRIC_PREFIX}forecast_end") == 0.25
        assert _value(wide, "2026-09-01", f"{NULL_METRIC_PREFIX}zone_name") == 0.0
        assert _value(wide, "2026-09-02", f"{NULL_METRIC_PREFIX}forecast_start") == 0.5
        assert _value(wide, "2026-09-02", f"{NULL_METRIC_PREFIX}forecast_end") == 0.5

    def test_bad_percentages(self, wide):
        assert _value(wide, "2026-09-01", f"{BAD_METRIC_PREFIX}danger_level_domain") == 0.25
        assert _value(wide, "2026-09-01", f"{BAD_METRIC_PREFIX}not_blank__zone_name") == 0.25
        assert _value(wide, "2026-09-01", f"{BAD_METRIC_PREFIX}file_date_iso") == 0.0
        assert _value(wide, "2026-09-02", f"{BAD_METRIC_PREFIX}danger_level_domain") == 0.0

    def test_nulls_are_not_double_counted_as_bad(self, wide):
        assert _value(wide, "2026-09-01", f"{BAD_METRIC_PREFIX}forecast_order") == 0.0

    def test_bad_row_pct(self, wide):
        assert _value(wide, "2026-09-01", BAD_ROW_METRIC) == 0.5
        assert _value(wide, "2026-09-02", BAD_ROW_METRIC) == 0.0

    def test_no_rating_sentinel_is_not_a_violation(self, spark_session):
        rows = [(1, -1, "Alpha", dt.datetime(2026, 9, 3, 10), dt.datetime(2026, 9, 3, 12), "2026-09-03")]
        df = spark_session.createDataFrame([Row(**dict(zip(COLUMNS, row, strict=True))) for row in rows])
        wide = compute_metrics(df, table_name="avalanche_org", model=AvalancheOrg)
        assert _value(wide, "2026-09-03", f"{BAD_METRIC_PREFIX}danger_level_domain") == 0.0
        assert _value(wide, "2026-09-03", BAD_ROW_METRIC) == 0.0

    def test_duplicate_and_unique_percentages(self, wide):
        assert _value(wide, "2026-09-01", DUP_ROW_METRIC) == 0.25
        assert _value(wide, "2026-09-01", UNIQUE_ROW_METRIC) == 0.75
        assert _value(wide, "2026-09-02", DUP_ROW_METRIC) == 0.0
        assert _value(wide, "2026-09-02", UNIQUE_ROW_METRIC) == 1.0

    def test_missing_file_date_raises(self, source_df):
        df = source_df.drop(FILE_DATE_COL)
        with pytest.raises(ValueError, match="file_date"):
            compute_metrics(df, table_name="avalanche_org", model=AvalancheOrg)


class TestToMetricsRows:
    def test_column_layout(self, wide):
        rows = to_metrics_rows(wide, table_name="avalanche_org")
        assert rows.columns == ["table_name", FILE_DATE_COL, "metric", "value", "computed_at"]

    def test_metric_count(self, wide):
        rows = to_metrics_rows(wide, table_name="avalanche_org")
        assert rows.count() == 2 * METRICS_PER_DATE

    def test_table_name_stamped_on_every_row(self, wide):
        rows = to_metrics_rows(wide, table_name="avalanche_org")
        assert {r.table_name for r in rows.collect()} == {"avalanche_org"}

    def test_values_are_numeric(self, wide):
        rows = to_metrics_rows(wide, table_name="avalanche_org")
        assert all(isinstance(r.value, float) for r in rows.collect())

    def test_total_records_metric_value(self, wide):
        rows = to_metrics_rows(wide, table_name="avalanche_org")
        found = [r.value for r in rows.collect() if r.metric == TOTAL_RECORDS_METRIC]
        assert sorted(found) == [2.0, 4.0]


class TestDataQualityRows:
    def test_matches_model_schema(self, source_df):
        rows = data_quality_rows(source_df, table_name="avalanche_org", model=AvalancheOrg)
        actual = [f.dataType for f in rows.schema.fields]
        expected = [f.dataType for f in pydantic_to_spark_schema(DataQuality).fields]
        assert [f.name for f in rows.schema.fields] == [f.name for f in pydantic_to_spark_schema(DataQuality).fields]
        assert actual == expected

    def test_metric_names(self, source_df):
        rows = data_quality_rows(source_df, table_name="avalanche_org", model=AvalancheOrg)
        names = {r.metric for r in rows.collect()}
        assert names == {
            TOTAL_RECORDS_METRIC,
            BAD_ROW_METRIC,
            DUP_ROW_METRIC,
            UNIQUE_ROW_METRIC,
            f"{NULL_METRIC_PREFIX}zone_id",
            f"{NULL_METRIC_PREFIX}danger_level",
            f"{NULL_METRIC_PREFIX}zone_name",
            f"{NULL_METRIC_PREFIX}forecast_start",
            f"{NULL_METRIC_PREFIX}forecast_end",
            f"{NULL_METRIC_PREFIX}file_date",
            f"{BAD_METRIC_PREFIX}not_blank__zone_name",
            f"{BAD_METRIC_PREFIX}file_date_iso",
            f"{BAD_METRIC_PREFIX}danger_level_domain",
            f"{BAD_METRIC_PREFIX}forecast_order",
        }

    def test_file_dates_present(self, source_df):
        rows = data_quality_rows(source_df, table_name="avalanche_org", model=AvalancheOrg)
        assert {r.file_date for r in rows.collect()} == {"2026-09-01", "2026-09-02"}

    def test_computed_at_is_set(self, source_df):
        rows = data_quality_rows(source_df, table_name="avalanche_org", model=AvalancheOrg)
        assert all(r.computed_at is not None for r in rows.collect())


def test_dt_annotation_used_by_model():
    assert DataQuality.model_fields["computed_at"].annotation is dt.datetime
