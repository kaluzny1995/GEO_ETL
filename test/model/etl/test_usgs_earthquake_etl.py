import datetime as dt

import pytest
from pydantic import ValidationError
from pyspark.sql.types import DoubleType, StringType, TimestampType

from factory import TableFactory
from model.etl.event_etl import EventEtl
from model.etl.usgs_earthquake_etl import UsgsEarthquakeEtl
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema


class TestUsgsEarthquakeEtlValid:
    def test_full_row(self):
        row = UsgsEarthquakeEtl(
            source_name="USGS",
            subsource_name="ci",
            event_name="Earthquake - United States",
            event_category="earthquake",
            event_subcategory="earthquake",
            event_description="earthquake reviewed",
            latitude=33.704166666667,
            longitude=-116.80866666667,
            country_code="US",
            state_province="CI",
            place_location="9 km WSW of Idyllwild, CA",
            metric_name="magnitude",
            metric_value=0.52,
            metric_unit="ml",
            danger_rating=None,
            other_metrics='{"depth":15.95,"depth_unit":"km","nst":15,"nst_unit":"stations"}',
            event_start=dt.datetime(2026, 9, 1, 23, 58, 46, 50000),
            event_end=None,
            last_latitude=None,
            last_longitude=None,
            file_date="2026-09-01",
        )
        assert row.source_name == "USGS"
        assert row.subsource_name == "ci"
        assert row.event_name == "Earthquake - United States"
        assert row.event_category == "earthquake"
        assert row.event_subcategory == "earthquake"
        assert row.event_description == "earthquake reviewed"
        assert row.country_code == "US"
        assert row.state_province == "CI"
        assert row.place_location == "9 km WSW of Idyllwild, CA"
        assert row.metric_name == "magnitude"
        assert row.metric_value == 0.52
        assert row.metric_unit == "ml"
        assert row.other_metrics == '{"depth":15.95,"depth_unit":"km","nst":15,"nst_unit":"stations"}'
        assert row.event_end is None

    def test_optional_fields_default_to_none(self):
        row = UsgsEarthquakeEtl(
            source_name="USGS",
            event_category="earthquake",
            country_code="US",
            metric_name="magnitude",
            file_date="2026-09-01",
        )
        assert row.subsource_name is None
        assert row.event_name is None
        assert row.event_subcategory is None
        assert row.event_description is None
        assert row.latitude is None
        assert row.longitude is None
        assert row.state_province is None
        assert row.place_location is None
        assert row.metric_value is None
        assert row.metric_unit is None
        assert row.danger_rating is None
        assert row.other_metrics is None
        assert row.event_start is None
        assert row.event_end is None
        assert row.last_latitude is None
        assert row.last_longitude is None


class TestUsgsEarthquakeEtlMissing:
    @pytest.mark.parametrize(
        "field",
        ["source_name", "event_category", "country_code", "metric_name", "file_date"],
    )
    def test_missing_required_field_raises(self, field):
        values = {
            "source_name": "USGS",
            "event_category": "earthquake",
            "country_code": "US",
            "metric_name": "magnitude",
            "file_date": "2026-09-01",
        }
        values.pop(field)
        with pytest.raises(ValidationError):
            UsgsEarthquakeEtl(**values)

    def test_empty_model_raises(self):
        with pytest.raises(ValidationError):
            UsgsEarthquakeEtl()


class TestUsgsEarthquakeEtlSchema:
    def test_inherits_the_canonical_structure(self):
        assert list(UsgsEarthquakeEtl.model_fields) == list(EventEtl.model_fields)

    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(UsgsEarthquakeEtl)
        assert len(schema.fields) == 21

    def test_schema_field_names(self):
        schema = pydantic_to_spark_schema(UsgsEarthquakeEtl)
        names = [f.name for f in schema.fields]
        assert names == [
            "source_name",
            "subsource_name",
            "event_name",
            "event_category",
            "event_subcategory",
            "event_description",
            "latitude",
            "longitude",
            "country_code",
            "state_province",
            "place_location",
            "metric_name",
            "metric_value",
            "metric_unit",
            "danger_rating",
            "other_metrics",
            "event_start",
            "event_end",
            "last_latitude",
            "last_longitude",
            "file_date",
        ]

    def test_string_fields(self):
        schema = pydantic_to_spark_schema(UsgsEarthquakeEtl)
        for name in ("source_name", "country_code", "place_location", "other_metrics", "file_date"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, StringType), name

    def test_double_fields(self):
        schema = pydantic_to_spark_schema(UsgsEarthquakeEtl)
        for name in ("latitude", "longitude", "metric_value", "last_latitude", "last_longitude"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, DoubleType), name

    def test_datetime_fields(self):
        schema = pydantic_to_spark_schema(UsgsEarthquakeEtl)
        for name in ("event_start", "event_end"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, TimestampType), name

    def test_required_fields_are_not_nullable(self):
        schema = pydantic_to_spark_schema(UsgsEarthquakeEtl)
        required = {"source_name", "event_category", "country_code", "metric_name", "file_date"}
        for f in schema.fields:
            assert f.nullable is (f.name not in required), f.name


class TestUsgsEarthquakeEtlDdl:
    def test_ddl_header(self):
        ddl = pydantic_to_ddl(UsgsEarthquakeEtl, "usgs_earthquake_etl", "geo")
        assert ddl.startswith("CREATE TABLE IF NOT EXISTS geo.usgs_earthquake_etl (")

    def test_ddl_column_types(self):
        ddl = pydantic_to_ddl(UsgsEarthquakeEtl, "usgs_earthquake_etl", "geo")
        assert "    country_code TEXT NOT NULL" in ddl
        assert "    place_location TEXT NULL" in ddl
        assert "    metric_value DOUBLE PRECISION NULL" in ddl
        assert "    event_start TIMESTAMP NULL" in ddl
        assert "    file_date TEXT NOT NULL" in ddl

    def test_registered_in_table_factory(self):
        TableFactory.register("usgs_earthquake_etl", UsgsEarthquakeEtl)
        assert "usgs_earthquake_etl" in TableFactory.tables()
        assert TableFactory.get("usgs_earthquake_etl") is UsgsEarthquakeEtl
