import datetime as dt
import json

import pytest
from pyspark.sql import Row

from model.etl.avalanche_org_etl import AvalancheOrgEtl
from single_etl.avalanche_org_etl import AvalancheOrgEtlScript
from utils.etl_utils import dedup

_SCRIPT = AvalancheOrgEtlScript()


def _source_row(**overrides):
    row = {
        "zone_id": 3004,
        "zone_name": "Bridgeport",
        "center_id": "BAC",
        "center_name": "Bridgeport Avalanche Center",
        "state": "CA",
        "danger_level": 4,
        "danger_rating": "high",
        "travel_advice": "Very dangerous avalanche conditions.",
        "off_season": False,
        "forecast_start": dt.datetime(2026, 1, 10, 6, 0, 0),
        "forecast_end": dt.datetime(2026, 1, 11, 6, 0, 0),
        "warning": "",
        "link": "https://example.org/zone",
        "latitude": 38.5,
        "longitude": -119.5,
        "avalanche_size": 3.0,
        "date": dt.date(2026, 1, 10),
        "file_date": "2026-01-10",
    }
    row.update(overrides)
    return Row(**row)


def _convert(spark, *rows):
    return _SCRIPT._convert(spark.createDataFrame(list(rows))).collect()


class TestConvert:
    def test_column_order_matches_the_model(self, spark_session):
        df = _SCRIPT._convert(spark_session.createDataFrame([_source_row()]))
        assert df.columns == list(AvalancheOrgEtl.model_fields)

    def test_zone_mapping(self, spark_session):
        (row,) = _convert(spark_session, _source_row())

        assert row.source_name == "AVALANCHE_ORG"
        assert row.subsource_name == "Bridgeport Avalanche Center"
        assert row.event_name is None
        assert row.place_location == "Bridgeport"
        assert row.event_category == "avalanche"
        assert row.event_subcategory is None
        assert row.event_description is None
        assert row.latitude == 38.5 and row.longitude == -119.5
        assert row.country_code == "US"
        assert row.state_province == "CA"
        assert row.metric_name == "size"
        assert row.metric_value == 3.0
        assert row.metric_unit == "destructive size class"
        assert row.danger_rating == "high"
        assert json.loads(row.other_metrics) == {
            "danger_level": 4,
            "danger_level_unit": "NAPADS level",
            "off_season": False,
        }
        assert row.event_start == dt.datetime(2026, 1, 10, 6, 0, 0)
        assert row.event_end == dt.datetime(2026, 1, 11, 6, 0, 0)
        assert row.last_latitude is None and row.last_longitude is None
        assert row.file_date == "2026-01-10"

    @pytest.mark.parametrize(
        ("danger_level", "danger_text", "expected_rating"),
        [
            (1, "low", "low"),
            (2, "moderate", "medium"),
            (3, "considerable", "high"),
            (4, "high", "high"),
            (5, "extreme", "extreme"),
            (-1, "no rating", "no_rating"),
            (-1, "no_rating", "no_rating"),
            (-1, "", "no_rating"),
        ],
    )
    def test_the_rating_is_the_napads_level_on_the_shared_scale(
        self, spark_session, danger_level, danger_text, expected_rating
    ):
        (row,) = _convert(spark_session, _source_row(danger_level=danger_level, danger_rating=danger_text))

        assert row.danger_rating == expected_rating

    def test_unrated_zone_rates_no_rating(self, spark_session):
        (row,) = _convert(spark_session, _source_row(danger_level=-1, danger_rating="no rating"))

        assert row.danger_rating == "no_rating"

    def test_expected_size_becomes_the_metric_with_its_unit(self, spark_session):
        (row,) = _convert(spark_session, _source_row(avalanche_size=3.0))

        assert row.metric_value == 3.0
        assert row.metric_unit == "destructive size class"

    def test_half_step_size_passes_through(self, spark_session):
        (row,) = _convert(spark_session, _source_row(avalanche_size=2.5))

        assert row.metric_value == 2.5
        assert row.metric_unit == "destructive size class"

    def test_zone_without_a_published_size_gets_null_metric_and_unit(self, spark_session):
        schema = spark_session.createDataFrame([_source_row()]).schema
        df = spark_session.createDataFrame([_source_row(avalanche_size=None)], schema=schema)
        (row,) = _SCRIPT._convert(df).collect()

        assert row.metric_value is None
        assert row.metric_unit is None

    def test_unrated_zone_with_underscored_rating_rates_no_rating(self, spark_session):
        (row,) = _convert(spark_session, _source_row(danger_level=-1, danger_rating="no_rating"))

        assert row.danger_rating == "no_rating"

    def test_blank_rating_rates_no_rating(self, spark_session):
        (row,) = _convert(spark_session, _source_row(danger_level=-1, danger_rating=""))

        assert row.danger_rating == "no_rating"

    def test_a_level_the_map_does_not_know_rates_no_rating(self, spark_session):
        (row,) = _convert(spark_session, _source_row(danger_level=7, danger_rating="something else"))

        assert row.danger_rating == "no_rating"

    def test_other_metrics_keep_the_warning_and_the_season_state(self, spark_session):
        (row,) = _convert(
            spark_session,
            _source_row(danger_level=5, danger_rating="extreme", off_season=True, warning="warning"),
        )

        assert json.loads(row.other_metrics) == {
            "danger_level": 5,
            "danger_level_unit": "NAPADS level",
            "warning": "warning",
            "off_season": True,
        }

    def test_other_metrics_keep_the_no_rating_sentinel(self, spark_session):
        (row,) = _convert(spark_session, _source_row(danger_level=-1, danger_rating="no rating", off_season=True))

        assert row.danger_rating == "no_rating"
        assert json.loads(row.other_metrics) == {
            "danger_level": -1,
            "danger_level_unit": "NAPADS level",
            "off_season": True,
        }

    def test_row_without_source_coordinates_gets_null_coordinates(self, spark_session):
        schema = spark_session.createDataFrame([_source_row()]).schema
        df = spark_session.createDataFrame([_source_row(latitude=None, longitude=None)], schema=schema)
        (row,) = _SCRIPT._convert(df).collect()

        assert row.latitude is None
        assert row.longitude is None

    def test_source_coordinates_pass_through_unchanged(self, spark_session):
        (row,) = _convert(spark_session, _source_row(latitude=43.75, longitude=-110.5))

        assert row.latitude == 43.75
        assert row.longitude == -110.5

    def test_analysis_day_and_next_day_rows_collapse_to_one(self, spark_session):
        rows = [
            _source_row(date=dt.date(2026, 1, 10)),
            _source_row(date=dt.date(2026, 1, 11)),
        ]

        converted = _SCRIPT._convert(spark_session.createDataFrame(list(rows)))
        out = dedup(converted, AvalancheOrgEtlScript.DEDUP_KEYS).collect()

        assert len(out) == 1
        assert out[0].file_date == "2026-01-10"

    def test_rows_with_different_forecast_window_are_kept(self, spark_session):
        rows = [
            _source_row(),
            _source_row(
                date=dt.date(2026, 1, 11),
                forecast_start=dt.datetime(2026, 1, 11, 6, 0, 0),
                forecast_end=dt.datetime(2026, 1, 12, 6, 0, 0),
            ),
        ]

        converted = _SCRIPT._convert(spark_session.createDataFrame(list(rows)))
        out = dedup(converted, AvalancheOrgEtlScript.DEDUP_KEYS).collect()

        assert len(out) == 2
        assert {row.event_start for row in out} == {
            dt.datetime(2026, 1, 10, 6, 0, 0),
            dt.datetime(2026, 1, 11, 6, 0, 0),
        }
