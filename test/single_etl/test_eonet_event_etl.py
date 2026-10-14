import datetime as dt

import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType

from model.etl.eonet_event_etl import EonetEventEtl
from model.plain.eonet_event import EonetEvent
from single_etl.eonet_event_etl import EonetEventEtlScript
from utils.schema_converter import pydantic_to_spark_schema

_SCRIPT = EonetEventEtlScript()


def _source_row(**overrides):
    row = {
        "event_id": "EONET_1234",
        "event_name": "Hurricane Polo",
        "category_id": "severeStorms",
        "category_name": "Severe Storms",
        "description": None,
        "source_id": "EO",
        "source_url": "https://example.org/eonet/1234",
        "latitude": 15.1433,
        "longitude": -104.7983,
        "event_date": dt.datetime(2026, 9, 20, 12, 0, 0),
        "magnitude_value": 60.0,
        "magnitude_unit": "kts",
        "closed": None,
        "geometry_count": 36,
        "last_latitude": 16.0,
        "last_longitude": -105.0,
        "last_date": dt.datetime(2026, 9, 21, 12, 0, 0),
        "geocode_country": None,
        "geocode_state": None,
        "file_date": "2026-09-21",
    }
    row.update(overrides)
    return Row(**row)


def _source_df(spark, *rows):
    schema = StructType([StructField(f.name, f.dataType, True) for f in pydantic_to_spark_schema(EonetEvent).fields])
    return spark.createDataFrame(list(rows), schema=schema)


def _convert(spark, *rows):
    return _SCRIPT._convert(_source_df(spark, *rows)).collect()


class TestConvert:
    def test_column_order_matches_the_model(self, spark_session):
        df = _SCRIPT._convert(_source_df(spark_session, _source_row()))
        assert df.columns == list(EonetEventEtl.model_fields)

    def test_storm_mapping(self, spark_session):
        (row,) = _convert(spark_session, _source_row())

        assert row.source_name == "EONET"
        assert row.subsource_name == "EO"
        assert row.event_name == "Hurricane Polo"
        assert row.event_category == "storm"
        assert row.event_subcategory == "hurricane"
        assert row.event_description is None
        assert row.latitude == 15.1433 and row.longitude == -104.7983
        assert row.country_code == "ZZ"
        assert row.state_province is None
        assert row.place_location is None
        assert row.metric_name == "wind speed"
        assert row.metric_value == 111.12
        assert row.metric_unit == "km/h"
        assert row.danger_rating == "no_rating"
        assert row.other_metrics == '{"geometry_count":36}'
        assert row.event_start == dt.datetime(2026, 9, 20, 12, 0, 0)
        assert row.event_end == dt.datetime(2026, 9, 21, 12, 0, 0)
        assert row.last_latitude == 16.0 and row.last_longitude == -105.0
        assert row.file_date == "2026-09-21"

    @pytest.mark.parametrize(
        ("name", "expected_place"),
        [
            ("Wildfire Snow, Custer, Montana", "Snow, Custer, Montana"),
            ("Prescribed Fire RX CALV BU 104-108, Vernon, Louisiana", "RX CALV BU 104-108, Vernon, Louisiana"),
            ("Incident Complex Rattlesnake Complex, Prairie, Montana", "Rattlesnake Complex, Prairie, Montana"),
            (
                "Emergency Stabilization BAER McConnell, Humboldt, Nevada",
                "McConnell, Humboldt, Nevada",
            ),
            ("Hurricane Polo", None),
            ("Iceberg D32", None),
        ],
    )
    def test_place_location_extraction(self, spark_session, name, expected_place):
        (row,) = _convert(spark_session, _source_row(event_name=name))

        assert row.place_location == expected_place

    def test_fire_maps_category_state_and_metric_from_the_description(self, spark_session):
        (row,) = _convert(
            spark_session,
            _source_row(
                event_name="Wildfire Snow, Custer, Montana",
                category_id="wildfires",
                category_name="Wildfires",
                description="30 Miles E from Miles City, MT",
                source_id="IRWIN",
                magnitude_value=120.0,
                magnitude_unit="acres",
            ),
        )

        assert row.event_category == "fire"
        assert row.event_subcategory == "wildfire"
        assert row.place_location == "Snow, Custer, Montana"
        assert row.country_code == "US"
        assert row.state_province == "MT"
        assert row.metric_name == "area"
        assert row.metric_value == 0.485622770688
        assert row.metric_unit == "km²"

    def test_state_and_country_from_the_stamped_geocode_when_the_description_has_none(self, spark_session):
        (row,) = _convert(spark_session, _source_row(geocode_country="US", geocode_state="MT"))

        assert row.country_code == "US"
        assert row.state_province == "MT"

    def test_the_description_state_beats_the_stamped_geocode(self, spark_session):
        (row,) = _convert(
            spark_session,
            _source_row(description="3 Miles E from Meridian, TX", geocode_country="MX", geocode_state="GRO"),
        )

        assert row.country_code == "US"
        assert row.state_province == "TX"

    def test_stamped_country_without_subdivision_leaves_state_null(self, spark_session):
        (row,) = _convert(spark_session, _source_row(geocode_country="PH", geocode_state=""))

        assert row.country_code == "PH"
        assert row.state_province is None

    def test_sea_and_lake_ice_event(self, spark_session):
        (row,) = _convert(
            spark_session,
            _source_row(
                event_name="Iceberg B22A",
                category_id="seaLakeIce",
                category_name="Sea and Lake Ice",
                magnitude_value=5.0,
                magnitude_unit="NM^2",
            ),
        )

        assert row.event_category == "iceberg"
        assert row.event_subcategory == "sea_lake_ice"
        assert row.metric_name == "surface"
        assert row.metric_value == 5.0
        assert row.metric_unit == "N/m²"
        assert row.place_location is None

    def test_missing_magnitude_counts_as_zero(self, spark_session):
        (row,) = _convert(spark_session, _source_row(magnitude_value=None, magnitude_unit=None))

        assert row.metric_value == 0.0
        assert row.metric_unit is None

    def test_ice_event_gets_the_si_unit_even_when_the_source_unit_is_blank(self, spark_session):
        (row,) = _convert(
            spark_session,
            _source_row(category_id="seaLakeIce", magnitude_value=None, magnitude_unit=""),
        )

        assert row.metric_value == 0.0
        assert row.metric_unit == "N/m²"

    def test_storm_without_a_type_keyword_has_no_subcategory(self, spark_session):
        (row,) = _convert(spark_session, _source_row(event_name="Storm System 42"))

        assert row.event_category == "storm"
        assert row.event_subcategory is None

    def test_tropical_cyclone_storm_subcategory(self, spark_session):
        (row,) = _convert(spark_session, _source_row(event_name="Tropical Cyclone 01B"))

        assert row.event_subcategory == "tropical cyclone"

    def test_unknown_category_is_snake_cased(self, spark_session):
        (row,) = _convert(
            spark_session,
            _source_row(event_name="Volcano X", category_id="volcanicAsh", category_name="Volcanic Ash"),
        )

        assert row.event_category == "volcanic_ash"
        assert row.event_subcategory is None
        assert row.metric_name == "magnitude"

    def test_blank_description_and_unit_stay_null(self, spark_session):
        (row,) = _convert(spark_session, _source_row(description="   ", magnitude_unit=""))

        assert row.event_description is None
        assert row.metric_unit is None

    def test_missing_geometry_count_keeps_other_metrics_null(self, spark_session):
        (row,) = _convert(spark_session, _source_row(geometry_count=None))

        assert row.other_metrics is None
