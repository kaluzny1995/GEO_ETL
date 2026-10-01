import datetime as dt

import pytest
from pydantic import ValidationError
from pyspark.sql.types import DateType, DoubleType, StringType, TimestampType

from factory import TableFactory
from model.etl.eonet_event_etl import EonetEventEtl
from model.etl.event_etl import EventEtl
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema


class TestEonetEventEtlValid:
    def test_full_row(self):
        row = EonetEventEtl(
            source_name="EONET",
            subsource_name="EO",
            event_name="Hurricane Polo",
            event_category="storm",
            event_subcategory="hurricane",
            event_description="Near the coast of Mexico.",
            latitude=15.1433,
            longitude=-104.7983,
            country_code="MX",
            state_province="GRO",
            place_location=None,
            metric_name="wind speed",
            metric_value=60.0,
            metric_unit="kts",
            danger_rating=None,
            statement=None,
            other_metrics='{"geometry_count":36}',
            event_start_date=dt.date(2026, 9, 20),
            event_start=dt.datetime(2026, 9, 20, 12, 0, 0),
            event_end=dt.datetime(2026, 9, 21, 12, 0, 0),
            last_latitude=16.0,
            last_longitude=-105.0,
            file_date="2026-09-21",
        )
        assert row.source_name == "EONET"
        assert row.event_category == "storm"
        assert row.event_subcategory == "hurricane"
        assert row.metric_name == "wind speed"
        assert row.metric_value == 60.0
        assert row.place_location is None
        assert row.danger_rating is None

    def test_optional_fields_default_to_none(self):
        row = EonetEventEtl(
            source_name="EONET",
            event_category="fire",
            country_code="US",
            metric_name="area",
            file_date="2026-09-21",
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
        assert row.other_metrics is None
        assert row.event_start_date is None
        assert row.event_start is None
        assert row.event_end is None
        assert row.last_latitude is None
        assert row.last_longitude is None


class TestEonetEventEtlMissing:
    @pytest.mark.parametrize(
        "field",
        ["source_name", "event_category", "country_code", "metric_name", "file_date"],
    )
    def test_missing_required_field_raises(self, field):
        values = {
            "source_name": "EONET",
            "event_category": "storm",
            "country_code": "ZZ",
            "metric_name": "wind speed",
            "file_date": "2026-09-21",
        }
        values.pop(field)
        with pytest.raises(ValidationError):
            EonetEventEtl(**values)

    def test_empty_model_raises(self):
        with pytest.raises(ValidationError):
            EonetEventEtl()


class TestEonetEventEtlSchema:
    def test_inherits_the_canonical_structure(self):
        assert list(EonetEventEtl.model_fields) == list(EventEtl.model_fields)

    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(EonetEventEtl)
        assert len(schema.fields) == 23

    def test_schema_field_names(self):
        schema = pydantic_to_spark_schema(EonetEventEtl)
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
            "statement",
            "other_metrics",
            "event_start_date",
            "event_start",
            "event_end",
            "last_latitude",
            "last_longitude",
            "file_date",
        ]

    def test_string_fields(self):
        schema = pydantic_to_spark_schema(EonetEventEtl)
        for name in ("source_name", "country_code", "place_location", "other_metrics", "file_date"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, StringType), name

    def test_double_fields(self):
        schema = pydantic_to_spark_schema(EonetEventEtl)
        for name in ("latitude", "longitude", "metric_value", "last_latitude", "last_longitude"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, DoubleType), name

    def test_date_field(self):
        schema = pydantic_to_spark_schema(EonetEventEtl)
        field = next(f for f in schema.fields if f.name == "event_start_date")
        assert isinstance(field.dataType, DateType)

    def test_datetime_fields(self):
        schema = pydantic_to_spark_schema(EonetEventEtl)
        for name in ("event_start", "event_end"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, TimestampType), name

    def test_required_fields_are_not_nullable(self):
        schema = pydantic_to_spark_schema(EonetEventEtl)
        required = {"source_name", "event_category", "country_code", "metric_name", "file_date"}
        for f in schema.fields:
            assert f.nullable is (f.name not in required), f.name


class TestEonetEventEtlDdl:
    def test_ddl_header(self):
        ddl = pydantic_to_ddl(EonetEventEtl, "eonet_event_etl", "geo")
        assert ddl.startswith("CREATE TABLE IF NOT EXISTS geo.eonet_event_etl (")

    def test_ddl_column_types(self):
        ddl = pydantic_to_ddl(EonetEventEtl, "eonet_event_etl", "geo")
        assert "    country_code TEXT NOT NULL" in ddl
        assert "    place_location TEXT NULL" in ddl
        assert "    metric_value DOUBLE PRECISION NULL" in ddl
        assert "    event_start_date DATE NULL" in ddl
        assert "    event_start TIMESTAMP NULL" in ddl
        assert "    file_date TEXT NOT NULL" in ddl

    def test_registered_in_table_factory(self):
        TableFactory.register("eonet_event_etl", EonetEventEtl)
        assert "eonet_event_etl" in TableFactory.tables()
        assert TableFactory.get("eonet_event_etl") is EonetEventEtl
