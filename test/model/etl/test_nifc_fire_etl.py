import datetime as dt

import pytest
from pydantic import ValidationError
from pyspark.sql.types import DoubleType, StringType, TimestampType

from factory import TableFactory
from model.etl.event_etl import EventEtl
from model.etl.nifc_fire_etl import NifcFireEtl
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema


class TestNifcFireEtlValid:
    def test_full_row(self):
        row = NifcFireEtl(
            source_name="NIFC",
            subsource_name="WFIGS",
            event_name="Iron Fire",
            event_category="fire",
            event_subcategory="wildfire",
            event_description=None,
            latitude=40.017760846821886,
            longitude=-112.14269529210874,
            country_code="US",
            state_province="UT",
            place_location="Okanogan-Wenatchee National Forest",
            metric_name="area",
            metric_value=1500.5,
            metric_unit="acres",
            danger_rating=None,
            other_metrics='{"percent_contained":45,"percent_contained_unit":"%"}',
            event_start=dt.datetime(2026, 8, 10, 17, 1, 9),
            event_end=dt.datetime(2026, 9, 17, 12, 0, 0),
            last_latitude=None,
            last_longitude=None,
            file_date="2026-09-01",
        )
        assert row.source_name == "NIFC"
        assert row.subsource_name == "WFIGS"
        assert row.event_name == "Iron Fire"
        assert row.event_category == "fire"
        assert row.event_subcategory == "wildfire"
        assert row.country_code == "US"
        assert row.state_province == "UT"
        assert row.metric_name == "area"
        assert row.metric_value == 1500.5
        assert row.metric_unit == "acres"
        assert row.other_metrics == '{"percent_contained":45,"percent_contained_unit":"%"}'
        assert row.event_end == dt.datetime(2026, 9, 17, 12, 0, 0)

    def test_optional_fields_default_to_none(self):
        row = NifcFireEtl(
            source_name="NIFC",
            event_category="fire",
            country_code="US",
            metric_name="area",
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


class TestNifcFireEtlMissing:
    @pytest.mark.parametrize(
        "field",
        ["source_name", "event_category", "country_code", "metric_name", "file_date"],
    )
    def test_missing_required_field_raises(self, field):
        values = {
            "source_name": "NIFC",
            "event_category": "fire",
            "country_code": "US",
            "metric_name": "area",
            "file_date": "2026-09-01",
        }
        values.pop(field)
        with pytest.raises(ValidationError):
            NifcFireEtl(**values)

    def test_empty_model_raises(self):
        with pytest.raises(ValidationError):
            NifcFireEtl()


class TestNifcFireEtlSchema:
    def test_inherits_the_canonical_structure(self):
        assert list(NifcFireEtl.model_fields) == list(EventEtl.model_fields)

    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(NifcFireEtl)
        assert len(schema.fields) == 21

    def test_schema_field_names(self):
        schema = pydantic_to_spark_schema(NifcFireEtl)
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
        schema = pydantic_to_spark_schema(NifcFireEtl)
        for name in ("source_name", "country_code", "place_location", "other_metrics", "file_date"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, StringType), name

    def test_double_fields(self):
        schema = pydantic_to_spark_schema(NifcFireEtl)
        for name in ("latitude", "longitude", "metric_value", "last_latitude", "last_longitude"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, DoubleType), name

    def test_datetime_fields(self):
        schema = pydantic_to_spark_schema(NifcFireEtl)
        for name in ("event_start", "event_end"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, TimestampType), name

    def test_required_fields_are_not_nullable(self):
        schema = pydantic_to_spark_schema(NifcFireEtl)
        required = {"source_name", "event_category", "country_code", "metric_name", "file_date"}
        for f in schema.fields:
            assert f.nullable is (f.name not in required), f.name


class TestNifcFireEtlDdl:
    def test_ddl_header(self):
        ddl = pydantic_to_ddl(NifcFireEtl, "nifc_fire_etl", "geo")
        assert ddl.startswith("CREATE TABLE IF NOT EXISTS geo.nifc_fire_etl (")

    def test_ddl_column_types(self):
        ddl = pydantic_to_ddl(NifcFireEtl, "nifc_fire_etl", "geo")
        assert "    country_code TEXT NOT NULL" in ddl
        assert "    place_location TEXT NULL" in ddl
        assert "    metric_value DOUBLE PRECISION NULL" in ddl
        assert "    event_start TIMESTAMP NULL" in ddl
        assert "    file_date TEXT NOT NULL" in ddl

    def test_registered_in_table_factory(self):
        TableFactory.register("nifc_fire_etl", NifcFireEtl)
        assert "nifc_fire_etl" in TableFactory.tables()
        assert TableFactory.get("nifc_fire_etl") is NifcFireEtl
