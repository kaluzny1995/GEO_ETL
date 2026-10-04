import datetime as dt

import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType

from model.etl.usgs_nwis_etl import UsgsNwisEtl
from model.plain.usgs_nwis import UsgsNwis
from single_etl.usgs_nwis_etl import UsgsNwisEtlScript
from utils.schema_converter import pydantic_to_spark_schema

_SCRIPT = UsgsNwisEtlScript()


def _source_row(**overrides):
    row = {
        "site_id": "USGS-01646500",
        "site_name": "POTOMAC RIVER NEAR WASH, DC LITTLE FALLS PUMP STA",
        "parameter_code": "00060",
        "date": dt.date(2026, 9, 1),
        "value": 1450.0,
        "unit": "ft3/s",
        "qualifiers": "P",
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


def _convert(spark, *rows):
    return _SCRIPT._convert(_source_df(spark, *rows)).collect()


class TestConvert:
    def test_column_order_matches_the_model(self, spark_session):
        df = _SCRIPT._convert(_source_df(spark_session, _source_row()))
        assert df.columns == list(UsgsNwisEtl.model_fields)

    def test_event_mapping(self, spark_session):
        (row,) = _convert(spark_session, _source_row())

        assert row.source_name == "USGS"
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
        assert row.metric_value == 1450.0
        assert row.metric_unit == "ft3/s"
        assert row.danger_rating is None
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
            (1450.0, 1450.0),
            (None, 0.0),
        ],
    )
    def test_metric_value_is_the_daily_reading(self, spark_session, value, expected_value):
        (row,) = _convert(spark_session, _source_row(value=value))

        assert row.metric_name == "discharge"
        assert row.metric_value == expected_value
        assert row.metric_unit == "ft3/s"

    @pytest.mark.parametrize("site_id", ["USGS-01646500", "USGS-03105500"])
    def test_subsource_name_is_the_site_id(self, spark_session, site_id):
        (row,) = _convert(spark_session, _source_row(site_id=site_id))

        assert row.subsource_name == site_id
