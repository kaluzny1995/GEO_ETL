import datetime as dt

import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType

from model.etl.openmeteo_flood_etl import OpenMeteoFloodEtl
from model.plain.openmeteo_flood import OpenMeteoFlood
from single_etl.openmeteo_flood_etl import OpenMeteoFloodEtlScript
from utils.schema_converter import pydantic_to_spark_schema

_SCRIPT = OpenMeteoFloodEtlScript()


def _source_row(**overrides):
    row = {
        "river_name": "Amazon",
        "country": "Brazil",
        "latitude": -3.14,
        "longitude": -59.98,
        "date": dt.date(2026, 9, 1),
        "discharge_m3s": 77982.05,
        "geocode_country": None,
        "geocode_state": None,
        "file_date": "2026-09-01",
    }
    row.update(overrides)
    return Row(**row)


def _source_df(spark, *rows):
    schema = StructType(
        [StructField(f.name, f.dataType, True) for f in pydantic_to_spark_schema(OpenMeteoFlood).fields]
    )
    return spark.createDataFrame(list(rows), schema=schema)


def _convert(spark, *rows):
    return _SCRIPT._convert(_source_df(spark, *rows)).collect()


class TestConvert:
    def test_column_order_matches_the_model(self, spark_session):
        df = _SCRIPT._convert(_source_df(spark_session, _source_row()))
        assert df.columns == list(OpenMeteoFloodEtl.model_fields)

    def test_river_mapping(self, spark_session):
        (row,) = _convert(spark_session, _source_row())

        assert row.source_name == "OPENMETEO"
        assert row.subsource_name == "GLOFAS"
        assert row.event_name == "Amazon discharge"
        assert row.event_category == "flood"
        assert row.event_subcategory is None
        assert row.event_description is None
        assert row.latitude == -3.14
        assert row.longitude == -59.98
        assert row.country_code == "BR"
        assert row.state_province is None
        assert row.place_location == "Amazon - Brazil"
        assert row.metric_name == "discharge"
        assert row.metric_value == 77982.05
        assert row.metric_unit == "m³/s"
        assert row.danger_rating is None
        assert row.other_metrics is None
        assert row.event_start == dt.datetime(2026, 9, 1, 0, 0)
        assert row.event_end is None
        assert row.last_latitude is None and row.last_longitude is None
        assert row.file_date == "2026-09-01"

    @pytest.mark.parametrize(
        ("country", "expected_code"),
        [
            ("Brazil", "BR"),
            ("USA", "US"),
            ("UK", "GB"),
            ("DRC", "CD"),
            ("Papua New Guinea", "PG"),
        ],
    )
    def test_country_name_maps_to_the_iso_code(self, spark_session, country, expected_code):
        (row,) = _convert(spark_session, _source_row(country=country))

        assert row.country_code == expected_code

    def test_known_country_name_wins_over_the_stamped_geocode(self, spark_session):
        (row,) = _convert(spark_session, _source_row(country="Brazil", geocode_country="AR", geocode_state="C"))

        assert row.country_code == "BR"

    def test_unknown_country_name_falls_back_to_the_stamped_geocode(self, spark_session):
        (row,) = _convert(spark_session, _source_row(country="Atlantis", geocode_country="AR", geocode_state="C"))

        assert row.country_code == "AR"

    def test_unknown_country_without_a_stamped_geocode_is_zz(self, spark_session):
        (row,) = _convert(spark_session, _source_row(country="Atlantis"))

        assert row.country_code == "ZZ"

    def test_state_province_comes_from_the_stamped_geocode(self, spark_session):
        (row,) = _convert(spark_session, _source_row(geocode_country="BR", geocode_state="AM"))

        assert row.state_province == "AM"

    @pytest.mark.parametrize("state", ["", None])
    def test_missing_stamped_geocode_state_is_null(self, spark_session, state):
        (row,) = _convert(spark_session, _source_row(geocode_country="BR", geocode_state=state))

        assert row.state_province is None

    @pytest.mark.parametrize("river_name", [None, "", "   "])
    def test_blank_river_name_is_null(self, spark_session, river_name):
        (row,) = _convert(spark_session, _source_row(river_name=river_name))

        assert row.event_name is None

    @pytest.mark.parametrize("country", [None, "", "   "])
    def test_blank_country_drops_that_part_of_the_place(self, spark_session, country):
        (row,) = _convert(spark_session, _source_row(country=country))

        assert row.place_location == "Amazon"

    @pytest.mark.parametrize("river_name", [None, "", "   "])
    def test_blank_river_keeps_the_country_as_the_place(self, spark_session, river_name):
        (row,) = _convert(spark_session, _source_row(river_name=river_name))

        assert row.place_location == "Brazil"

    def test_both_place_parts_blank_is_null(self, spark_session):
        (row,) = _convert(spark_session, _source_row(river_name=None, country=None))

        assert row.place_location is None

    @pytest.mark.parametrize(
        ("discharge_m3s", "expected_value"),
        [
            (77982.05, 77982.05),
            (0.0, 0.0),
            (None, 0.0),
        ],
    )
    def test_metric_value_is_the_discharge(self, spark_session, discharge_m3s, expected_value):
        (row,) = _convert(spark_session, _source_row(discharge_m3s=discharge_m3s))

        assert row.metric_name == "discharge"
        assert row.metric_value == expected_value
        assert row.metric_unit == "m³/s"
