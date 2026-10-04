import datetime as dt

import pytest
from pydantic import ValidationError
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    TimestampType,
)

from factory import TableFactory
from model.plain.gdacs_event import GdacsEvent
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema


class TestGdacsEventValid:
    def test_valid_row(self):
        row = GdacsEvent(
            event_type="WF",
            event_id=1031770,
            event_name="Forest fires in Australia",
            alert_level="Green",
            country="Australia",
            country_code="AUS",
            latitude=-15.7068,
            longitude=130.5705,
            from_date=dt.datetime(2026, 9, 2, 0, 0, 0),
            to_date=dt.datetime(2026, 9, 17, 0, 0, 0),
            date_modified=dt.datetime(2026, 9, 17, 19, 35, 40),
            severity=14196.0,
            severity_text="Green impact for forestfire in 14196 ha",
            severity_unit="ha",
            source="GWIS",
            report_url="https://www.gdacs.org/report.aspx?eventid=1031770",
            is_current=True,
            file_date="2026-09-01",
        )
        assert row.event_id == 1031770
        assert row.is_current is True
        assert row.file_date == "2026-09-01"

    def test_optional_fields_none(self):
        row = GdacsEvent(
            event_type="TC",
            event_id=1001323,
            event_name="Tropical Cyclone FAY-26",
            alert_level="Green",
            latitude=30.8,
            longitude=-35.3,
            from_date=dt.datetime(2026, 9, 19, 15, 0, 0),
            to_date=dt.datetime(2026, 9, 22, 21, 0, 0),
            date_modified=dt.datetime(2026, 9, 22, 20, 50, 45),
            severity=111.1104,
            source="NOAA",
            report_url="https://www.gdacs.org/report.aspx?eventid=1001323",
            is_current=True,
            file_date="2026-09-22",
        )
        assert row.country is None
        assert row.country_code is None
        assert row.severity_text is None
        assert row.severity_unit is None


class TestGdacsEventMissing:
    def test_missing_required_field_raises(self):
        with pytest.raises(ValidationError):
            GdacsEvent(event_name="test")

    def test_missing_event_id_raises(self):
        with pytest.raises(ValidationError):
            GdacsEvent(
                event_type="WF",
                event_name="test",
                alert_level="Green",
                latitude=0.0,
                longitude=0.0,
                from_date=dt.datetime(2026, 1, 1),
                to_date=dt.datetime(2026, 1, 1),
                date_modified=dt.datetime(2026, 1, 1),
                severity=0.0,
                source="test",
                report_url="http://test.com",
                is_current=True,
                file_date="2026-01-01",
            )


class TestGdacsEventSparkSchema:
    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(GdacsEvent)
        assert len(schema.fields) == 20

    def test_schema_field_names(self):
        schema = pydantic_to_spark_schema(GdacsEvent)
        names = [f.name for f in schema.fields]
        assert names == [
            "event_type",
            "event_id",
            "event_name",
            "alert_level",
            "country",
            "country_code",
            "latitude",
            "longitude",
            "from_date",
            "to_date",
            "date_modified",
            "severity",
            "severity_text",
            "severity_unit",
            "source",
            "report_url",
            "is_current",
            "geocode_country",
            "geocode_state",
            "file_date",
        ]

    def test_double_fields(self):
        schema = pydantic_to_spark_schema(GdacsEvent)
        double_fields = {"latitude", "longitude", "severity"}
        for f in schema.fields:
            if f.name in double_fields:
                assert isinstance(f.dataType, DoubleType), f"{f.name} should be DoubleType"

    def test_int_field(self):
        schema = pydantic_to_spark_schema(GdacsEvent)
        event_id = next(f for f in schema.fields if f.name == "event_id")
        assert isinstance(event_id.dataType, LongType)

    def test_timestamp_fields(self):
        schema = pydantic_to_spark_schema(GdacsEvent)
        for name in ("from_date", "to_date", "date_modified"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, TimestampType), f"{name} should be TimestampType"

    def test_bool_field(self):
        schema = pydantic_to_spark_schema(GdacsEvent)
        is_current = next(f for f in schema.fields if f.name == "is_current")
        assert isinstance(is_current.dataType, BooleanType)

    def test_nullable_fields(self):
        schema = pydantic_to_spark_schema(GdacsEvent)
        nullable_names = {
            "country",
            "country_code",
            "severity_text",
            "severity_unit",
            "geocode_country",
            "geocode_state",
        }
        for f in schema.fields:
            if f.name in nullable_names:
                assert f.nullable is True, f"{f.name} should be nullable"
            else:
                assert f.nullable is False, f"{f.name} should not be nullable"


class TestGdacsEventDDL:
    def test_ddl_types(self):
        ddl = pydantic_to_ddl(GdacsEvent, "gdacs_event", schema="geo")
        assert "event_type TEXT NOT NULL" in ddl
        assert "event_id INTEGER NOT NULL" in ddl
        assert "latitude DOUBLE PRECISION NOT NULL" in ddl
        assert "longitude DOUBLE PRECISION NOT NULL" in ddl
        assert "from_date TIMESTAMP NOT NULL" in ddl
        assert "to_date TIMESTAMP NOT NULL" in ddl
        assert "date_modified TIMESTAMP NOT NULL" in ddl
        assert "severity DOUBLE PRECISION NOT NULL" in ddl
        assert "is_current BOOLEAN NOT NULL" in ddl
        assert "country TEXT NULL" in ddl
        assert "file_date TEXT NOT NULL" in ddl

    def test_ddl_creates_correct_table(self):
        ddl = pydantic_to_ddl(GdacsEvent, "gdacs_event", schema="geo")
        assert "CREATE TABLE IF NOT EXISTS geo.gdacs_event" in ddl


class TestGdacsEventRegistry:
    def test_registered_in_factory(self):
        import model  # noqa: F401
        from model.plain.gdacs_event import GdacsEvent as GdacsEventModel

        TableFactory.register("gdacs_event", GdacsEventModel)
        assert "gdacs_event" in TableFactory.tables()
        assert TableFactory.get("gdacs_event") is GdacsEventModel
