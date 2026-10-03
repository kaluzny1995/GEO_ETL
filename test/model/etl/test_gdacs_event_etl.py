import datetime as dt

import pytest
from pydantic import ValidationError
from pyspark.sql.types import DoubleType, StringType, TimestampType

from factory import TableFactory
from model.etl.event_etl import EventEtl
from model.etl.gdacs_event_etl import GdacsEventEtl
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema


class TestGdacsEventEtlValid:
    def test_full_row(self):
        row = GdacsEventEtl(
            source_name="GDACS",
            subsource_name="NEIC",
            event_name="Earthquake in Japan",
            event_category="earthquake",
            event_subcategory=None,
            event_description=None,
            latitude=35.42,
            longitude=139.45,
            country_code="JP",
            state_province="13",
            place_location="Japan",
            metric_name="magnitude",
            metric_value=5.4,
            metric_unit="M",
            danger_rating="Orange",
            other_metrics=None,
            event_start=dt.datetime(2026, 9, 29, 21, 50, 29),
            event_end=dt.datetime(2026, 9, 29, 21, 50, 29),
            last_latitude=None,
            last_longitude=None,
            file_date="2026-09-29",
        )
        assert row.source_name == "GDACS"
        assert row.event_category == "earthquake"
        assert row.subsource_name == "NEIC"
        assert row.metric_name == "magnitude"
        assert row.metric_value == 5.4
        assert row.place_location == "Japan"
        assert row.danger_rating == "Orange"
        assert row.other_metrics is None
        assert row.last_latitude is None

    def test_optional_fields_default_to_none(self):
        row = GdacsEventEtl(
            source_name="GDACS",
            event_category="storm",
            country_code="ZZ",
            metric_name="wind speed",
            file_date="2026-09-29",
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


class TestGdacsEventEtlMissing:
    @pytest.mark.parametrize(
        "field",
        ["source_name", "event_category", "country_code", "metric_name", "file_date"],
    )
    def test_missing_required_field_raises(self, field):
        values = {
            "source_name": "GDACS",
            "event_category": "earthquake",
            "country_code": "ZZ",
            "metric_name": "magnitude",
            "file_date": "2026-09-29",
        }
        values.pop(field)
        with pytest.raises(ValidationError):
            GdacsEventEtl(**values)

    def test_empty_model_raises(self):
        with pytest.raises(ValidationError):
            GdacsEventEtl()


class TestGdacsEventEtlSchema:
    def test_inherits_the_canonical_structure(self):
        assert list(GdacsEventEtl.model_fields) == list(EventEtl.model_fields)

    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(GdacsEventEtl)
        assert len(schema.fields) == 21

    def test_schema_field_names(self):
        schema = pydantic_to_spark_schema(GdacsEventEtl)
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
        schema = pydantic_to_spark_schema(GdacsEventEtl)
        for name in ("source_name", "country_code", "place_location", "other_metrics", "file_date"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, StringType), name

    def test_double_fields(self):
        schema = pydantic_to_spark_schema(GdacsEventEtl)
        for name in ("latitude", "longitude", "metric_value", "last_latitude", "last_longitude"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, DoubleType), name

    def test_datetime_fields(self):
        schema = pydantic_to_spark_schema(GdacsEventEtl)
        for name in ("event_start", "event_end"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, TimestampType), name

    def test_required_fields_are_not_nullable(self):
        schema = pydantic_to_spark_schema(GdacsEventEtl)
        required = {"source_name", "event_category", "country_code", "metric_name", "file_date"}
        for f in schema.fields:
            assert f.nullable is (f.name not in required), f.name


class TestGdacsEventEtlDdl:
    def test_ddl_header(self):
        ddl = pydantic_to_ddl(GdacsEventEtl, "gdacs_event_etl", "geo")
        assert ddl.startswith("CREATE TABLE IF NOT EXISTS geo.gdacs_event_etl (")

    def test_ddl_column_types(self):
        ddl = pydantic_to_ddl(GdacsEventEtl, "gdacs_event_etl", "geo")
        assert "    country_code TEXT NOT NULL" in ddl
        assert "    place_location TEXT NULL" in ddl
        assert "    metric_value DOUBLE PRECISION NULL" in ddl
        assert "    event_start TIMESTAMP NULL" in ddl
        assert "    file_date TEXT NOT NULL" in ddl

    def test_registered_in_table_factory(self):
        TableFactory.register("gdacs_event_etl", GdacsEventEtl)
        assert "gdacs_event_etl" in TableFactory.tables()
        assert TableFactory.get("gdacs_event_etl") is GdacsEventEtl
