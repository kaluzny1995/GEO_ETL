import datetime as dt

import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType

from model.etl.nifc_fire_etl import NifcFireEtl
from model.plain.nifc_fire import NifcFire
from single_etl.nifc_fire_etl import NifcFireEtlScript
from utils.schema_converter import pydantic_to_spark_schema

_SCRIPT = NifcFireEtlScript()


def _source_row(**overrides):
    row = {
        "incident_name": "Iron Fire",
        "irwin_id": "{B22F7798-E3C3-4E1B-A091-CC85EBE4DA75}",
        "fire_number": None,
        "acres": 1500.5,
        "percent_contained": 45.0,
        "fire_type": "WF",
        "fire_type_label": "Wildfire Daily Fire Perimeter",
        "jurisdiction": "USFS",
        "jurisdiction_label": "WAOWF",
        "unit_name": "Okanogan-Wenatchee National Forest",
        "state": "UT",
        "po_method": "US-UT",
        "latitude": 40.017760846821886,
        "longitude": -112.14269529210874,
        "poly_date": dt.datetime(2026, 9, 1, 12, 1, 2),
        "created_on": dt.datetime(2026, 8, 10, 17, 1, 9),
        "modified_on": dt.datetime(2026, 9, 15, 8, 0, 0),
        "geocode_state": None,
        "file_date": "2026-09-01",
    }
    row.update(overrides)
    return Row(**row)


def _source_df(spark, *rows):
    schema = StructType([StructField(f.name, f.dataType, True) for f in pydantic_to_spark_schema(NifcFire).fields])
    return spark.createDataFrame(list(rows), schema=schema)


def _convert(spark, *rows):
    return _SCRIPT._convert(_source_df(spark, *rows)).collect()


class TestConvert:
    def test_column_order_matches_the_model(self, spark_session):
        df = _SCRIPT._convert(_source_df(spark_session, _source_row()))
        assert df.columns == list(NifcFireEtl.model_fields)

    def test_fire_mapping(self, spark_session):
        (row,) = _convert(spark_session, _source_row())

        assert row.source_name == "NIFC"
        assert row.subsource_name == "WFIGS"
        assert row.event_name == "Iron Fire"
        assert row.event_category == "fire"
        assert row.event_subcategory == "wildfire"
        assert row.event_description is None
        assert row.latitude == 40.017760846821886
        assert row.longitude == -112.14269529210874
        assert row.country_code == "US"
        assert row.state_province == "UT"
        assert row.place_location == "Okanogan-Wenatchee National Forest"
        assert row.metric_name == "area"
        assert row.metric_value == 6.0723080618112
        assert row.metric_unit == "km²"
        assert row.danger_rating == "no_rating"
        assert row.other_metrics == '{"percent_contained":45,"percent_contained_unit":"%"}'
        assert row.event_start == dt.datetime(2026, 8, 10, 17, 1, 9)
        assert row.event_end is None
        assert row.last_latitude is None and row.last_longitude is None
        assert row.file_date == "2026-09-01"

    @pytest.mark.parametrize(
        ("fire_type", "expected_subcategory"),
        [
            ("WF", "wildfire"),
            ("RX", "prescribed fire"),
            (None, "fire"),
            ("", "fire"),
            ("XX", "fire"),
        ],
    )
    def test_subcategory_comes_from_fire_type(self, spark_session, fire_type, expected_subcategory):
        (row,) = _convert(spark_session, _source_row(fire_type=fire_type))

        assert row.event_subcategory == expected_subcategory

    @pytest.mark.parametrize("incident_name", [None, "", "   "])
    def test_blank_incident_name_is_null(self, spark_session, incident_name):
        (row,) = _convert(spark_session, _source_row(incident_name=incident_name))

        assert row.event_name is None

    def test_source_state_wins_over_the_stamped_geocode(self, spark_session):
        (row,) = _convert(spark_session, _source_row(state="UT", geocode_state="NM"))

        assert row.state_province == "UT"

    @pytest.mark.parametrize("state", [None, "", "   "])
    def test_blank_state_falls_back_to_the_stamped_geocode(self, spark_session, state):
        (row,) = _convert(spark_session, _source_row(state=state, geocode_state="NM"))

        assert row.state_province == "NM"

    def test_blank_state_without_a_stamped_geocode_is_null(self, spark_session):
        (row,) = _convert(spark_session, _source_row(state=None))

        assert row.state_province is None

    def test_unit_name_passes_through_unchanged(self, spark_session):
        (row,) = _convert(spark_session, _source_row(jurisdiction_label="NVNV0100", unit_name="Okanogan NF"))

        assert row.place_location == "Okanogan NF"

    def test_stamped_raw_code_passes_through(self, spark_session):
        (row,) = _convert(spark_session, _source_row(jurisdiction_label="NVNV0100", unit_name="NVNV0100"))

        assert row.place_location == "NVNV0100"

    @pytest.mark.parametrize("unit_name", [None, "", "   "])
    def test_blank_unit_name_is_null(self, spark_session, unit_name):
        (row,) = _convert(spark_session, _source_row(unit_name=unit_name))

        assert row.place_location is None

    @pytest.mark.parametrize(
        ("percent_contained", "expected_json"),
        [
            (93.0, '{"percent_contained":93,"percent_contained_unit":"%"}'),
            (None, '{"percent_contained":0,"percent_contained_unit":"%"}'),
        ],
    )
    def test_other_metrics_always_report_containment_with_its_unit(
        self, spark_session, percent_contained, expected_json
    ):
        (row,) = _convert(spark_session, _source_row(percent_contained=percent_contained))

        assert row.other_metrics == expected_json

    @pytest.mark.parametrize(
        ("acres", "expected_value"),
        [
            (1500.5, 6.0723080618112),
            (None, 0.0),
        ],
    )
    def test_metric_value_is_the_acreage(self, spark_session, acres, expected_value):
        (row,) = _convert(spark_session, _source_row(acres=acres))

        assert row.metric_name == "area"
        assert row.metric_value == expected_value
        assert row.metric_unit == "km²"
