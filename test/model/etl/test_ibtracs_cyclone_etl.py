import datetime as dt

import pytest
from pydantic import ValidationError
from pyspark.sql.types import DoubleType, StringType, TimestampType

from factory import TableFactory
from model.etl.event_etl import EventEtl
from model.etl.ibtracs_cyclone_etl import IbtracsCycloneEtl
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema


class TestIbtracsCycloneEtlValid:
    def test_full_row(self):
        row = IbtracsCycloneEtl(
            source_name="IBTRACS",
            subsource_name="NA",
            event_name="EDOUARD",
            event_category="storm",
            event_subcategory="hurricane",
            event_description=None,
            latitude=29.9,
            longitude=-93.9,
            country_code="US",
            state_province="TX",
            place_location="North Atlantic - Gulf of Mexico",
            metric_name="wind speed",
            metric_value=65.0,
            metric_unit="kts",
            danger_rating="category 1",
            other_metrics='{"pressure": 985, "speed": 8}',
            event_start=dt.datetime(2026, 9, 1, 21, 0, 0),
            event_end=dt.datetime(2026, 9, 1, 21, 0, 0),
            last_latitude=29.9,
            last_longitude=-93.9,
            file_date="2026-09-01",
        )
        assert row.source_name == "IBTRACS"
        assert row.event_category == "storm"
        assert row.subsource_name == "NA"
        assert row.event_name == "EDOUARD"
        assert row.event_subcategory == "hurricane"
        assert row.metric_name == "wind speed"
        assert row.metric_value == 65.0
        assert row.metric_unit == "kts"
        assert row.danger_rating == "category 1"
        assert row.place_location == "North Atlantic - Gulf of Mexico"
        assert row.last_latitude == 29.9

    def test_optional_fields_default_to_none(self):
        row = IbtracsCycloneEtl(
            source_name="IBTRACS",
            event_category="storm",
            country_code="ZZ",
            metric_name="wind speed",
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


class TestIbtracsCycloneEtlMissing:
    @pytest.mark.parametrize(
        "field",
        ["source_name", "event_category", "country_code", "metric_name", "file_date"],
    )
    def test_missing_required_field_raises(self, field):
        values = {
            "source_name": "IBTRACS",
            "event_category": "storm",
            "country_code": "ZZ",
            "metric_name": "wind speed",
            "file_date": "2026-09-01",
        }
        values.pop(field)
        with pytest.raises(ValidationError):
            IbtracsCycloneEtl(**values)

    def test_empty_model_raises(self):
        with pytest.raises(ValidationError):
            IbtracsCycloneEtl()


class TestIbtracsCycloneEtlSchema:
    def test_inherits_the_canonical_structure(self):
        assert list(IbtracsCycloneEtl.model_fields) == list(EventEtl.model_fields)

    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(IbtracsCycloneEtl)
        assert len(schema.fields) == 21

    def test_schema_field_names(self):
        schema = pydantic_to_spark_schema(IbtracsCycloneEtl)
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
        schema = pydantic_to_spark_schema(IbtracsCycloneEtl)
        for name in ("source_name", "country_code", "place_location", "other_metrics", "file_date"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, StringType), name

    def test_double_fields(self):
        schema = pydantic_to_spark_schema(IbtracsCycloneEtl)
        for name in ("latitude", "longitude", "metric_value", "last_latitude", "last_longitude"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, DoubleType), name

    def test_datetime_fields(self):
        schema = pydantic_to_spark_schema(IbtracsCycloneEtl)
        for name in ("event_start", "event_end"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, TimestampType), name

    def test_required_fields_are_not_nullable(self):
        schema = pydantic_to_spark_schema(IbtracsCycloneEtl)
        required = {"source_name", "event_category", "country_code", "metric_name", "file_date"}
        for f in schema.fields:
            assert f.nullable is (f.name not in required), f.name


class TestIbtracsCycloneEtlDdl:
    def test_ddl_header(self):
        ddl = pydantic_to_ddl(IbtracsCycloneEtl, "ibtracs_cyclone_etl", "geo")
        assert ddl.startswith("CREATE TABLE IF NOT EXISTS geo.ibtracs_cyclone_etl (")

    def test_ddl_column_types(self):
        ddl = pydantic_to_ddl(IbtracsCycloneEtl, "ibtracs_cyclone_etl", "geo")
        assert "    country_code TEXT NOT NULL" in ddl
        assert "    place_location TEXT NULL" in ddl
        assert "    metric_value DOUBLE PRECISION NULL" in ddl
        assert "    event_start TIMESTAMP NULL" in ddl
        assert "    file_date TEXT NOT NULL" in ddl

    def test_registered_in_table_factory(self):
        TableFactory.register("ibtracs_cyclone_etl", IbtracsCycloneEtl)
        assert "ibtracs_cyclone_etl" in TableFactory.tables()
        assert TableFactory.get("ibtracs_cyclone_etl") is IbtracsCycloneEtl
