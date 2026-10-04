import datetime as dt

import pytest
from pydantic import BaseModel
from pyspark.sql import Row

from model.other.data_quality import DataQuality
from model.plain.avalanche_org import AvalancheOrg
from model.plain.usgs_earthquake import UsgsEarthquake
from utils.dq_metrics import (
    BAD_METRIC_PREFIX,
    BAD_ROW_METRIC,
    DISTINCT_METRIC_PREFIX,
    DUP_ROW_METRIC,
    FILE_DATE_COL,
    MAX_METRIC_PREFIX,
    MEAN_METRIC_PREFIX,
    METRICS_TABLE,
    MIN_METRIC_PREFIX,
    NULL_METRIC_PREFIX,
    OVERALL_FILE_DATE,
    PERCENTILE_POINTS,
    RANGE_METRIC_PREFIX,
    STDDEV_METRIC_PREFIX,
    TOTAL_RECORDS_METRIC,
    UNIQUE_ROW_METRIC,
    compute_metrics,
    data_quality_rows,
    dataset_rules,
    model_rules,
    numeric_columns,
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

NUMERIC_COLUMNS = ["zone_id", "danger_level"]
DISTINCT_COLUMNS = ["zone_id", "danger_level", "zone_name", "forecast_start", "forecast_end"]

METRICS_PER_DATE = 39

VARYINGNESS_PREFIXES = (
    MIN_METRIC_PREFIX,
    MAX_METRIC_PREFIX,
    RANGE_METRIC_PREFIX,
    MEAN_METRIC_PREFIX,
    STDDEV_METRIC_PREFIX,
    *(f"p{point:02d}__" for point in PERCENTILE_POINTS),
)


def _varyingness_metrics() -> set[str]:
    """Every range/spread/percentile/cardinality metric of the test frame."""
    return {f"{prefix}{column}" for column in NUMERIC_COLUMNS for prefix in VARYINGNESS_PREFIXES} | {
        f"{DISTINCT_METRIC_PREFIX}{column}" for column in DISTINCT_COLUMNS
    }


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
        assert OVERALL_FILE_DATE == "overall"
        assert METRICS_TABLE == "data_quality"
        assert TOTAL_RECORDS_METRIC == "total_records"
        assert NULL_METRIC_PREFIX == "null_pct__"
        assert BAD_METRIC_PREFIX == "bad_pct__"
        assert BAD_ROW_METRIC == "bad_row_pct"
        assert DUP_ROW_METRIC == "dup_row_pct"
        assert UNIQUE_ROW_METRIC == "unique_row_pct"

    def test_varyingness_metric_names(self):
        assert MIN_METRIC_PREFIX == "min__"
        assert MAX_METRIC_PREFIX == "max__"
        assert RANGE_METRIC_PREFIX == "range__"
        assert MEAN_METRIC_PREFIX == "mean__"
        assert STDDEV_METRIC_PREFIX == "stddev__"
        assert DISTINCT_METRIC_PREFIX == "distinct__"
        assert PERCENTILE_POINTS == (5, 25, 50, 75, 95)


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

    @pytest.mark.parametrize(
        "table_name, columns, expected_names",
        [
            ("eonet_event", ["geometry_count"], ["geometry_count_non_negative"]),
            ("gdacs_event", ["alert_level"], ["alert_level_domain"]),
            ("nifc_fire", ["acres", "percent_contained"], ["acres_non_negative", "percent_contained_0_100"]),
            ("openmeteo_flood", ["discharge_m3s"], ["discharge_non_negative"]),
        ],
    )
    def test_dataset_rules_should_add_dataset_specific_rules(self, spark_session, table_name, columns, expected_names):
        rules = dataset_rules(table_name, columns)
        assert [r.name for r in rules] == expected_names

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

    def test_overall_mode_drops_the_file_date_iso_rule(self, spark_session):
        assert [r.name for r in model_rules(AvalancheOrg, COLUMNS, overall=True)] == ["not_blank__zone_name"]

    def test_overall_mode_keeps_the_other_rules(self, spark_session):
        names = [r.name for r in rules_for("avalanche_org", AvalancheOrg, COLUMNS, overall=True)]
        assert names == ["not_blank__zone_name", "danger_level_domain", "forecast_order"]


class TestNumericColumns:
    def test_selects_numeric_model_columns(self):
        assert numeric_columns(AvalancheOrg, COLUMNS) == ["zone_id", "danger_level"]

    def test_keeps_model_field_order(self):
        assert numeric_columns(AvalancheOrg, ["danger_level", "zone_name", "zone_id", "file_date"]) == [
            "zone_id",
            "danger_level",
        ]

    def test_unwraps_optional_numeric(self):
        assert numeric_columns(UsgsEarthquake, ["latitude", "mag_type", "place", "file_date"]) == ["latitude"]

    def test_includes_bool_columns(self):
        assert numeric_columns(AvalancheOrg, ["off_season", "zone_name", "file_date"]) == ["off_season"]

    def test_excludes_file_date_and_missing_columns(self):
        assert numeric_columns(AvalancheOrg, ["zone_id"]) == ["zone_id"]
        assert numeric_columns(AvalancheOrg, ["file_date"]) == []
        assert numeric_columns(AvalancheOrg, ["zone_name"]) == []

    def test_excludes_temporal_and_string_columns(self):
        assert numeric_columns(AvalancheOrg, ["forecast_start", "forecast_end", "date", "zone_name"]) == []


class TestComputeMetrics:
    def test_columns(self, wide):
        expected = (
            [FILE_DATE_COL, TOTAL_RECORDS_METRIC]
            + [f"{NULL_METRIC_PREFIX}{c}" for c in COLUMNS]
            + [f"{BAD_METRIC_PREFIX}not_blank__zone_name", f"{BAD_METRIC_PREFIX}file_date_iso"]
            + [f"{BAD_METRIC_PREFIX}danger_level_domain", f"{BAD_METRIC_PREFIX}forecast_order"]
            + [BAD_ROW_METRIC, DUP_ROW_METRIC, UNIQUE_ROW_METRIC]
            + list(_varyingness_metrics())
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


class TestVaryingnessMetrics:
    def test_min_max_range(self, wide):
        assert _value(wide, "2026-09-01", f"{MIN_METRIC_PREFIX}zone_id") == 1.0
        assert _value(wide, "2026-09-01", f"{MAX_METRIC_PREFIX}zone_id") == 3.0
        assert _value(wide, "2026-09-01", f"{RANGE_METRIC_PREFIX}zone_id") == 2.0
        assert _value(wide, "2026-09-02", f"{MIN_METRIC_PREFIX}zone_id") == 4.0
        assert _value(wide, "2026-09-02", f"{MAX_METRIC_PREFIX}zone_id") == 5.0
        assert _value(wide, "2026-09-01", f"{MIN_METRIC_PREFIX}danger_level") == 1.0
        assert _value(wide, "2026-09-01", f"{MAX_METRIC_PREFIX}danger_level") == 9.0
        assert _value(wide, "2026-09-01", f"{RANGE_METRIC_PREFIX}danger_level") == 8.0

    def test_mean_and_stddev(self, wide):
        assert _value(wide, "2026-09-01", f"{MEAN_METRIC_PREFIX}zone_id") == pytest.approx(1.75)
        assert _value(wide, "2026-09-01", f"{STDDEV_METRIC_PREFIX}zone_id") == pytest.approx(0.82915619758885)
        assert _value(wide, "2026-09-01", f"{MEAN_METRIC_PREFIX}danger_level") == pytest.approx(4.0)
        assert _value(wide, "2026-09-01", f"{STDDEV_METRIC_PREFIX}danger_level") == pytest.approx(3.0)
        assert _value(wide, "2026-09-02", f"{MEAN_METRIC_PREFIX}zone_id") == pytest.approx(4.5)
        assert _value(wide, "2026-09-02", f"{STDDEV_METRIC_PREFIX}danger_level") == pytest.approx(0.5)

    def test_mean_stays_within_the_observed_range(self, wide):
        for file_date in ("2026-09-01", "2026-09-02"):
            for column in NUMERIC_COLUMNS:
                low = _value(wide, file_date, f"{MIN_METRIC_PREFIX}{column}")
                high = _value(wide, file_date, f"{MAX_METRIC_PREFIX}{column}")
                assert low <= _value(wide, file_date, f"{MEAN_METRIC_PREFIX}{column}") <= high
                assert _value(wide, file_date, f"{RANGE_METRIC_PREFIX}{column}") == pytest.approx(high - low)

    def test_percentiles_are_ordered_and_bounded(self, wide):
        for file_date in ("2026-09-01", "2026-09-02"):
            for column in NUMERIC_COLUMNS:
                quantiles = [_value(wide, file_date, f"p{point:02d}__{column}") for point in PERCENTILE_POINTS]
                assert quantiles == sorted(quantiles)
                assert _value(wide, file_date, f"{MIN_METRIC_PREFIX}{column}") <= quantiles[0]
                assert quantiles[-1] <= _value(wide, file_date, f"{MAX_METRIC_PREFIX}{column}")

    def test_percentiles_return_observed_values(self, wide):
        # Spark's percentile_approx never interpolates between observations.
        assert _value(wide, "2026-09-01", "p50__zone_id") == 1.0
        assert _value(wide, "2026-09-01", "p95__zone_id") == 3.0
        assert _value(wide, "2026-09-01", "p50__danger_level") == 3.0
        assert _value(wide, "2026-09-02", "p95__danger_level") == 2.0

    def test_distinct_counts(self, wide):
        assert _value(wide, "2026-09-01", f"{DISTINCT_METRIC_PREFIX}zone_id") == 3.0
        assert _value(wide, "2026-09-01", f"{DISTINCT_METRIC_PREFIX}danger_level") == 3.0
        assert _value(wide, "2026-09-01", f"{DISTINCT_METRIC_PREFIX}zone_name") == 3.0
        assert _value(wide, "2026-09-01", f"{DISTINCT_METRIC_PREFIX}forecast_start") == 1.0
        assert _value(wide, "2026-09-02", f"{DISTINCT_METRIC_PREFIX}zone_id") == 2.0

    def test_string_columns_only_get_cardinality(self, wide):
        assert f"{DISTINCT_METRIC_PREFIX}zone_name" in wide.columns
        for prefix in VARYINGNESS_PREFIXES:
            assert f"{prefix}zone_name" not in wide.columns

    def test_file_date_excluded_from_varyingness_metrics(self, wide):
        for prefix in VARYINGNESS_PREFIXES:
            assert f"{prefix}{FILE_DATE_COL}" not in wide.columns
        assert f"{DISTINCT_METRIC_PREFIX}{FILE_DATE_COL}" not in wide.columns

    def test_all_null_numeric_column_omits_undefined_stats(self, spark_session):
        rows = [
            (None, "3", "Alpha", "2026-09-01 10:00", "2026-09-01 12:00", "2026-09-01"),
            (None, "1", "Bravo", None, None, "2026-09-01"),
        ]
        schema = ",".join(f"{column} string" for column in COLUMNS)
        df = spark_session.createDataFrame(rows, schema)
        rows_long = data_quality_rows(df, table_name="avalanche_org", model=AvalancheOrg).collect()

        metrics = {row.metric for row in rows_long}
        assert f"{MIN_METRIC_PREFIX}zone_id" not in metrics
        assert f"{RANGE_METRIC_PREFIX}zone_id" not in metrics
        assert f"{MEAN_METRIC_PREFIX}zone_id" not in metrics
        assert f"{STDDEV_METRIC_PREFIX}zone_id" not in metrics
        assert "p50__zone_id" not in metrics
        assert f"{NULL_METRIC_PREFIX}zone_id" in metrics
        assert f"{DISTINCT_METRIC_PREFIX}zone_id" in metrics
        assert all(row.value is not None for row in rows_long)

    def test_stats_ignore_null_values(self, spark_session):
        rows = [
            ("1", "3", "Alpha", None, None, "2026-09-01"),
            (None, "9", "Bravo", None, None, "2026-09-01"),
        ]
        schema = ",".join(f"{column} string" for column in COLUMNS)
        df = spark_session.createDataFrame(rows, schema)
        wide = compute_metrics(df, table_name="avalanche_org", model=AvalancheOrg)
        assert _value(wide, "2026-09-01", f"{MIN_METRIC_PREFIX}zone_id") == 1.0
        assert _value(wide, "2026-09-01", f"{MAX_METRIC_PREFIX}zone_id") == 1.0
        assert _value(wide, "2026-09-01", f"{MEAN_METRIC_PREFIX}zone_id") == pytest.approx(1.0)
        assert _value(wide, "2026-09-01", f"{DISTINCT_METRIC_PREFIX}zone_id") == 1.0


class TestOverallMetrics:
    """``overall=True`` measures the whole table as one group."""

    def test_single_group_for_the_whole_table(self, source_df):
        wide = compute_metrics(source_df, table_name="avalanche_org", model=AvalancheOrg, overall=True)
        assert wide.count() == 1
        assert wide.collect()[0][FILE_DATE_COL] == OVERALL_FILE_DATE

    def test_total_records_covers_every_row(self, source_df):
        wide = compute_metrics(source_df, table_name="avalanche_org", model=AvalancheOrg, overall=True)
        assert _value(wide, OVERALL_FILE_DATE, TOTAL_RECORDS_METRIC) == len(ROWS)

    def test_file_date_iso_rule_is_not_applied(self, source_df):
        wide = compute_metrics(source_df, table_name="avalanche_org", model=AvalancheOrg, overall=True)
        assert f"{BAD_METRIC_PREFIX}file_date_iso" not in wide.columns
        assert f"{BAD_METRIC_PREFIX}danger_level_domain" in wide.columns
        assert f"{NULL_METRIC_PREFIX}{FILE_DATE_COL}" in wide.columns

    def test_rates_aggregate_the_whole_table(self, source_df):
        wide = compute_metrics(source_df, table_name="avalanche_org", model=AvalancheOrg, overall=True)
        assert _value(wide, OVERALL_FILE_DATE, BAD_ROW_METRIC) == pytest.approx(2 / len(ROWS))
        assert _value(wide, OVERALL_FILE_DATE, DUP_ROW_METRIC) == pytest.approx(1 / len(ROWS))
        assert _value(wide, OVERALL_FILE_DATE, UNIQUE_ROW_METRIC) == pytest.approx(5 / len(ROWS))
        assert _value(wide, OVERALL_FILE_DATE, f"{NULL_METRIC_PREFIX}forecast_start") == pytest.approx(2 / len(ROWS))

    def test_row_layout_uses_the_overall_sentinel(self, source_df):
        rows = data_quality_rows(source_df, table_name="avalanche_org", model=AvalancheOrg, overall=True).collect()
        assert {row.file_date for row in rows} == {OVERALL_FILE_DATE}
        assert len(rows) == METRICS_PER_DATE - 1  # one rule fewer than a dated run

    def test_source_frame_keeps_its_own_file_dates(self, source_df):
        compute_metrics(source_df, table_name="avalanche_org", model=AvalancheOrg, overall=True)
        assert {row[FILE_DATE_COL] for row in source_df.select(FILE_DATE_COL).distinct().collect()} == {
            "2026-09-01",
            "2026-09-02",
        }

    def test_default_matches_a_normal_run(self, source_df):
        assert data_quality_rows(source_df, table_name="avalanche_org", model=AvalancheOrg).columns == (
            data_quality_rows(source_df, table_name="avalanche_org", model=AvalancheOrg, overall=False).columns
        )


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
            *_varyingness_metrics(),
        }

    def test_file_dates_present(self, source_df):
        rows = data_quality_rows(source_df, table_name="avalanche_org", model=AvalancheOrg)
        assert {r.file_date for r in rows.collect()} == {"2026-09-01", "2026-09-02"}

    def test_computed_at_is_set(self, source_df):
        rows = data_quality_rows(source_df, table_name="avalanche_org", model=AvalancheOrg)
        assert all(r.computed_at is not None for r in rows.collect())


def test_dt_annotation_used_by_model():
    assert DataQuality.model_fields["computed_at"].annotation is dt.datetime
