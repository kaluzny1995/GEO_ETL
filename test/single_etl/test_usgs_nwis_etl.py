import csv
import datetime as dt
import json

import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType

from model.etl.usgs_nwis_etl import UsgsNwisEtl
from model.plain.usgs_nwis import UsgsNwis
from single_etl import usgs_nwis_etl as module
from single_etl.usgs_nwis_etl import UsgsNwisEtlScript
from utils.schema_converter import pydantic_to_spark_schema

_NO_REFERENCE = "/nonexistent/usgs_flood_categories.csv"

_SCRIPT = UsgsNwisEtlScript(flood_categories_path=_NO_REFERENCE)

_REFERENCE_FIELDS = [
    "site_id",
    "identifier",
    "site_name",
    "state",
    "latitude",
    "longitude",
    "action_stage_ft",
    "minor_stage_ft",
    "moderate_stage_ft",
    "major_stage_ft",
]

_REFERENCE_ROW = {
    "site_id": "USGS-01646500",
    "identifier": "BRKM2",
    "site_name": "Potomac River near Washington DC (Little Falls)",
    "state": "MD",
    "latitude": 38.949722222222,
    "longitude": -77.127777777778,
    "action_stage_ft": 5.0,
    "minor_stage_ft": 10.0,
    "moderate_stage_ft": 12.0,
    "major_stage_ft": 14.0,
}


def _source_row(**overrides):
    row = {
        "site_id": "USGS-01646500",
        "site_name": "POTOMAC RIVER NEAR WASH, DC LITTLE FALLS PUMP STA",
        "parameter_code": "00060",
        "date": dt.date(2026, 9, 1),
        "value": 1450.0,
        "unit": "ft3/s",
        "qualifiers": "P",
        "gauge_height": None,
        "flood_category": None,
        "latitude": 39.0016667,
        "longitude": -77.1591667,
        "geocode_state": None,
        "file_date": "2026-09-01",
    }
    row.update(overrides)
    return Row(**row)


def _source_df(spark, *rows):
    schema = StructType([StructField(f.name, f.dataType, True) for f in pydantic_to_spark_schema(UsgsNwis).fields])
    return spark.createDataFrame(list(rows), schema=schema)


def _reference_csv(tmp_path, **overrides):
    """Write a one-gauge flood-category reference and return its path."""
    row = {**_REFERENCE_ROW, **overrides}
    path = tmp_path / "usgs_flood_categories.csv"
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=_REFERENCE_FIELDS)
        writer.writeheader()
        writer.writerow(row)
    return str(path)


def _convert(spark, *rows, script=None):
    return (script or _SCRIPT)._convert(_source_df(spark, *rows)).collect()


class TestConvert:
    def test_column_order_matches_the_model(self, spark_session):
        df = _SCRIPT._convert(_source_df(spark_session, _source_row()))
        assert df.columns == list(UsgsNwisEtl.model_fields)

    def test_event_mapping(self, spark_session):
        (row,) = _convert(spark_session, _source_row())

        assert row.source_name == "USGS_NWIS"
        assert row.subsource_name == "USGS-01646500"
        assert row.event_name == "Streamflow - Potomac river"
        assert row.event_category == "flood"
        assert row.event_subcategory is None
        assert row.event_description == "provisional"
        assert row.latitude == 39.0016667
        assert row.longitude == -77.1591667
        assert row.country_code == "US"
        assert row.state_province is None
        assert row.place_location == "Potomac river, WASH, DC LITTLE FALLS PUMP STA"
        assert row.metric_name == "discharge"
        assert row.metric_value == 41.0594275584
        assert row.metric_unit == "m³/s"
        assert row.danger_rating == "no_rating"
        assert row.other_metrics is None
        assert row.event_start == dt.datetime(2026, 9, 1)
        assert row.event_end is None
        assert row.last_latitude is None and row.last_longitude is None
        assert row.file_date == "2026-09-01"

    @pytest.mark.parametrize(
        ("site_name", "expected_name", "expected_place"),
        [
            (
                "POTOMAC RIVER NEAR WASH, DC LITTLE FALLS PUMP STA",
                "Streamflow - Potomac river",
                "Potomac river, WASH, DC LITTLE FALLS PUMP STA",
            ),
            (
                "OHIO RIVER AT LOCK 1 NEAR PARKERSBURG, WV",
                "Streamflow - Ohio river",
                "Ohio river, LOCK 1 NEAR PARKERSBURG, WV",
            ),
            ("POTOMAC RIVER", "Streamflow - Potomac river", "Potomac river"),
        ],
    )
    def test_event_name_and_place_come_from_site_name(self, spark_session, site_name, expected_name, expected_place):
        (row,) = _convert(spark_session, _source_row(site_name=site_name))

        assert row.event_name == expected_name
        assert row.place_location == expected_place

    @pytest.mark.parametrize("site_name", [None, "", "   "])
    def test_blank_site_name_falls_back_to_the_plain_event_name(self, spark_session, site_name):
        (row,) = _convert(spark_session, _source_row(site_name=site_name))

        assert row.event_name == "Streamflow"
        assert row.place_location is None

    @pytest.mark.parametrize(
        ("qualifiers", "expected_description"),
        [
            ("P", "provisional"),
            ("A", "approved for publication"),
            ("A,e", "approved for publication, estimated"),
            ("P, e", "provisional, estimated"),
            ("ZFL", "zero flow"),
            ("X", "X"),
        ],
    )
    def test_description_expands_qualifier_codes_to_full_names(self, spark_session, qualifiers, expected_description):
        (row,) = _convert(spark_session, _source_row(qualifiers=qualifiers))

        assert row.event_description == expected_description

    @pytest.mark.parametrize("qualifiers", [None, "", "   "])
    def test_blank_qualifiers_have_no_description(self, spark_session, qualifiers):
        (row,) = _convert(spark_session, _source_row(qualifiers=qualifiers))

        assert row.event_description is None

    @pytest.mark.parametrize(
        ("geocode_state", "expected_state"),
        [
            ("DC", "DC"),
            ("", None),
            (None, None),
        ],
    )
    def test_state_province_is_the_stamped_geocode(self, spark_session, geocode_state, expected_state):
        (row,) = _convert(spark_session, _source_row(geocode_state=geocode_state))

        assert row.state_province == expected_state
        assert row.country_code == "US"

    @pytest.mark.parametrize(
        ("value", "expected_value"),
        [
            (1450.0, 41.0594275584),
            (None, 0.0),
        ],
    )
    def test_metric_value_is_the_daily_reading(self, spark_session, value, expected_value):
        (row,) = _convert(spark_session, _source_row(value=value))

        assert row.metric_name == "discharge"
        assert row.metric_value == expected_value
        assert row.metric_unit == "m³/s"

    @pytest.mark.parametrize("site_id", ["USGS-01646500", "USGS-03105500"])
    def test_subsource_name_is_the_site_id(self, spark_session, site_id):
        (row,) = _convert(spark_session, _source_row(site_id=site_id))

        assert row.subsource_name == site_id

    @pytest.mark.parametrize(
        ("flood_category", "expected_rating"),
        [
            ("no_flooding", "none"),
            ("action", "low"),
            ("minor", "medium"),
            ("moderate", "high"),
            ("major", "extreme"),
            (" Major ", "extreme"),
            ("", "no_rating"),
            (None, "no_rating"),
            ("unknown category", "no_rating"),
        ],
    )
    def test_the_rating_is_the_flood_category_on_the_shared_scale(self, spark_session, flood_category, expected_rating):
        (row,) = _convert(spark_session, _source_row(gauge_height=7.8, flood_category=flood_category))

        assert row.danger_rating == expected_rating

    def test_the_default_path_is_the_file_name_from_the_config(self):
        script = UsgsNwisEtlScript()

        assert script.flood_categories_path.endswith(module._cfg.flood_categories_file)
        assert script.flood_categories_path != _NO_REFERENCE

    def test_a_day_without_a_stage_or_a_reference_rates_no_rating(self, spark_session):
        (row,) = _convert(spark_session, _source_row())

        assert row.danger_rating == "no_rating"
        assert row.other_metrics is None

    def test_a_missing_reference_file_still_reports_the_peak(self, spark_session):
        (row,) = _convert(spark_session, _source_row(gauge_height=2.94, flood_category="no_flooding"))

        assert row.danger_rating == "none"
        assert json.loads(row.other_metrics) == {"gauge_height": 2.94, "gauge_height_unit": "ft"}


class TestFloodCategoryReference:
    def test_other_metrics_carries_the_peak_against_the_nws_thresholds(self, spark_session, tmp_path):
        script = UsgsNwisEtlScript(flood_categories_path=_reference_csv(tmp_path))

        (row,) = _convert(spark_session, _source_row(gauge_height=7.8), script=script)

        assert json.loads(row.other_metrics) == {
            "gauge_height": 7.8,
            "gauge_height_unit": "ft",
            "nws_gauge_id": "BRKM2",
            "action_stage": 5.0,
            "action_stage_unit": "ft",
            "minor_stage": 10.0,
            "minor_stage_unit": "ft",
            "moderate_stage": 12.0,
            "moderate_stage_unit": "ft",
            "major_stage": 14.0,
            "major_stage_unit": "ft",
        }

    def test_a_gauge_the_reference_does_not_cover_reports_only_the_peak(self, spark_session, tmp_path):
        script = UsgsNwisEtlScript(flood_categories_path=_reference_csv(tmp_path, site_id="USGS-03105500"))

        (row,) = _convert(spark_session, _source_row(gauge_height=7.8, flood_category="minor"), script=script)

        assert json.loads(row.other_metrics) == {"gauge_height": 7.8, "gauge_height_unit": "ft"}
        assert row.danger_rating == "medium"

    def test_a_reference_without_a_stage_of_its_own_reports_the_thresholds(self, spark_session, tmp_path):
        script = UsgsNwisEtlScript(flood_categories_path=_reference_csv(tmp_path))

        (row,) = _convert(spark_session, _source_row(gauge_height=None), script=script)

        metrics = json.loads(row.other_metrics)
        assert "gauge_height" not in metrics
        assert "gauge_height_unit" not in metrics
        assert metrics["nws_gauge_id"] == "BRKM2"
        assert metrics["action_stage"] == 5.0
        assert metrics["action_stage_unit"] == "ft"
        assert metrics["major_stage"] == 14.0
        assert metrics["major_stage_unit"] == "ft"

    def test_a_day_with_neither_a_stage_nor_a_covering_reference_has_no_other_metrics(self, spark_session, tmp_path):
        script = UsgsNwisEtlScript(flood_categories_path=_reference_csv(tmp_path, site_id="USGS-03105500"))

        (row,) = _convert(spark_session, _source_row(), script=script)

        assert row.other_metrics is None
        assert row.danger_rating == "no_rating"

    def test_no_unit_is_carried_inside_a_key_name(self, spark_session, tmp_path):
        script = UsgsNwisEtlScript(flood_categories_path=_reference_csv(tmp_path))

        (row,) = _convert(spark_session, _source_row(gauge_height=7.8), script=script)

        assert not any(key.endswith("_ft") for key in json.loads(row.other_metrics))

    def test_the_reference_does_not_shadow_the_gauge_columns(self, spark_session, tmp_path):
        script = UsgsNwisEtlScript(flood_categories_path=_reference_csv(tmp_path))

        (row,) = _convert(spark_session, _source_row(gauge_height=7.8), script=script)

        assert row.event_name == "Streamflow - Potomac river"
        assert row.latitude == 39.0016667
        assert row.longitude == -77.1591667
        assert row.state_province is None
