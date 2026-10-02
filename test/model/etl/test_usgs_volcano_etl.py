import datetime as dt

import pytest
from pydantic import ValidationError
from pyspark.sql.types import DateType, DoubleType, StringType, TimestampType

from factory import TableFactory
from model.etl.event_etl import EventEtl
from model.etl.usgs_volcano_etl import UsgsVolcanoEtl
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema


class TestUsgsVolcanoEtlValid:
    def test_full_row(self):
        row = UsgsVolcanoEtl(
            source_name="USGS",
            subsource_name="Alaska Volcano Observatory",
            event_name="Volcano - Great Sitkin",
            event_category="volcano",
            event_subcategory="Daily Update",
            event_description=None,
            latitude=52.0765,
            longitude=-176.1109,
            country_code="US",
            state_province="AK",
            place_location="Great Sitkin, Aleutians",
            metric_name="alert_level",
            metric_value=2.0,
            metric_unit=None,
            danger_rating="WATCH",
            statement="AVO Great Sitkin ORANGE/WATCH - Slow eruption of lava continues.",
            other_metrics=None,
            event_start_date=dt.date(2026, 9, 29),
            event_start=dt.datetime(2026, 9, 29, 18, 26, 19),
            event_end=None,
            last_latitude=None,
            last_longitude=None,
            file_date="2026-09-29",
        )
        assert row.source_name == "USGS"
        assert row.subsource_name == "Alaska Volcano Observatory"
        assert row.event_name == "Volcano - Great Sitkin"
        assert row.event_category == "volcano"
        assert row.event_subcategory == "Daily Update"
        assert row.event_description is None
        assert row.state_province == "AK"
        assert row.place_location == "Great Sitkin, Aleutians"
        assert row.metric_name == "alert_level"
        assert row.metric_value == 2.0
        assert row.danger_rating == "WATCH"
        assert row.other_metrics is None
        assert row.event_end is None

    def test_optional_fields_default_to_none(self):
        row = UsgsVolcanoEtl(
            source_name="USGS",
            event_category="volcano",
            country_code="US",
            metric_name="alert_level",
            file_date="2026-09-29",
        )
        assert row.subsource_name is None
        assert row.event_name is None
        assert row.event_subcategory is None
        assert row.latitude is None
        assert row.longitude is None
        assert row.state_province is None
        assert row.place_location is None
        assert row.metric_value is None
        assert row.metric_unit is None
        assert row.danger_rating is None
        assert row.statement is None
        assert row.event_start_date is None
        assert row.event_start is None
        assert row.event_end is None
        assert row.last_latitude is None
        assert row.last_longitude is None


class TestUsgsVolcanoEtlMissing:
    @pytest.mark.parametrize(
        "field",
        ["source_name", "event_category", "country_code", "metric_name", "file_date"],
    )
    def test_missing_required_field_raises(self, field):
        values = {
            "source_name": "USGS",
            "event_category": "volcano",
            "country_code": "US",
            "metric_name": "alert_level",
            "file_date": "2026-09-29",
        }
        values.pop(field)
        with pytest.raises(ValidationError):
            UsgsVolcanoEtl(**values)

    def test_empty_model_raises(self):
        with pytest.raises(ValidationError):
            UsgsVolcanoEtl()


class TestUsgsVolcanoEtlSchema:
    def test_inherits_the_canonical_structure(self):
        assert list(UsgsVolcanoEtl.model_fields) == list(EventEtl.model_fields)

    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(UsgsVolcanoEtl)
        assert len(schema.fields) == 23

    def test_schema_field_names(self):
        schema = pydantic_to_spark_schema(UsgsVolcanoEtl)
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
        schema = pydantic_to_spark_schema(UsgsVolcanoEtl)
        for name in ("source_name", "country_code", "place_location", "other_metrics", "file_date"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, StringType), name

    def test_double_fields(self):
        schema = pydantic_to_spark_schema(UsgsVolcanoEtl)
        for name in ("latitude", "longitude", "metric_value", "last_latitude", "last_longitude"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, DoubleType), name

    def test_date_field(self):
        schema = pydantic_to_spark_schema(UsgsVolcanoEtl)
        field = next(f for f in schema.fields if f.name == "event_start_date")
        assert isinstance(field.dataType, DateType)

    def test_datetime_fields(self):
        schema = pydantic_to_spark_schema(UsgsVolcanoEtl)
        for name in ("event_start", "event_end"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, TimestampType), name

    def test_required_fields_are_not_nullable(self):
        schema = pydantic_to_spark_schema(UsgsVolcanoEtl)
        required = {"source_name", "event_category", "country_code", "metric_name", "file_date"}
        for f in schema.fields:
            assert f.nullable is (f.name not in required), f.name


class TestUsgsVolcanoEtlDdl:
    def test_ddl_header(self):
        ddl = pydantic_to_ddl(UsgsVolcanoEtl, "usgs_volcano_etl", "geo")
        assert ddl.startswith("CREATE TABLE IF NOT EXISTS geo.usgs_volcano_etl (")

    def test_ddl_column_types(self):
        ddl = pydantic_to_ddl(UsgsVolcanoEtl, "usgs_volcano_etl", "geo")
        assert "    country_code TEXT NOT NULL" in ddl
        assert "    place_location TEXT NULL" in ddl
        assert "    metric_value DOUBLE PRECISION NULL" in ddl
        assert "    event_start_date DATE NULL" in ddl
        assert "    event_start TIMESTAMP NULL" in ddl
        assert "    file_date TEXT NOT NULL" in ddl

    def test_registered_in_table_factory(self):
        TableFactory.register("usgs_volcano_etl", UsgsVolcanoEtl)
        assert "usgs_volcano_etl" in TableFactory.tables()
        assert TableFactory.get("usgs_volcano_etl") is UsgsVolcanoEtl
