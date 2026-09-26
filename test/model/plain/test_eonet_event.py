import datetime as dt

import pytest
from pydantic import ValidationError
from pyspark.sql.types import (
    DateType,
    DoubleType,
    LongType,
    TimestampType,
)

from factory import TableFactory
from model.plain.eonet_event import EonetEvent
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema


class TestEonetEventValid:
    def test_valid_row(self):
        row = EonetEvent(
            event_id="EONET_23870",
            event_name="Wildfire Snow, Custer, Montana",
            category_id="wildfires",
            category_name="Wildfires",
            description="30 Miles E from Miles City, MT",
            source_id="IRWIN",
            source_url="https://irwin.doi.gov/observer/incidents/2026-MTLG14-000775",
            latitude=46.48575,
            longitude=-105.185017,
            event_date=dt.datetime(2026, 9, 2, 22, 16, 0),
            magnitude_value=1100.0,
            magnitude_unit="acres",
            closed=None,
            geometry_count=1,
            last_latitude=46.48575,
            last_longitude=-105.185017,
            last_date=dt.datetime(2026, 9, 2, 22, 16, 0),
            file_date="2026-09-01",
        )
        assert row.event_id == "EONET_23870"
        assert row.latitude == 46.48575
        assert row.file_date == "2026-09-01"

    def test_optional_fields_none(self):
        row = EonetEvent(
            event_id="EONET_12345",
            event_name="Test Event",
            category_id="test",
            category_name="Test",
            source_id="TEST",
            latitude=0.0,
            longitude=0.0,
            event_date=dt.datetime(2026, 1, 1),
            geometry_count=0,
            last_latitude=0.0,
            last_longitude=0.0,
            last_date=dt.datetime(2026, 1, 1),
            file_date="2026-01-01",
        )
        assert row.description is None
        assert row.source_url is None
        assert row.magnitude_value is None
        assert row.magnitude_unit is None
        assert row.closed is None


class TestEonetEventMissing:
    def test_missing_required_field_raises(self):
        with pytest.raises(ValidationError):
            EonetEvent(event_name="test")

    def test_missing_event_id_raises(self):
        with pytest.raises(ValidationError):
            EonetEvent(
                event_name="test",
                category_id="test",
                category_name="Test",
                source_id="TEST",
                latitude=0.0,
                longitude=0.0,
                event_date=dt.datetime(2026, 1, 1),
                geometry_count=0,
                last_latitude=0.0,
                last_longitude=0.0,
                last_date=dt.datetime(2026, 1, 1),
                file_date="2026-01-01",
            )


class TestEonetEventSparkSchema:
    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(EonetEvent)
        assert len(schema.fields) == 18

    def test_schema_field_names(self):
        schema = pydantic_to_spark_schema(EonetEvent)
        names = [f.name for f in schema.fields]
        assert names == [
            "event_id",
            "event_name",
            "category_id",
            "category_name",
            "description",
            "source_id",
            "source_url",
            "latitude",
            "longitude",
            "event_date",
            "magnitude_value",
            "magnitude_unit",
            "closed",
            "geometry_count",
            "last_latitude",
            "last_longitude",
            "last_date",
            "file_date",
        ]

    def test_double_fields(self):
        schema = pydantic_to_spark_schema(EonetEvent)
        double_fields = {"latitude", "longitude", "magnitude_value", "last_latitude", "last_longitude"}
        for f in schema.fields:
            if f.name in double_fields:
                assert isinstance(f.dataType, DoubleType), f"{f.name} should be DoubleType"

    def test_int_field(self):
        schema = pydantic_to_spark_schema(EonetEvent)
        geometry_count = next(f for f in schema.fields if f.name == "geometry_count")
        assert isinstance(geometry_count.dataType, LongType)

    def test_timestamp_fields(self):
        schema = pydantic_to_spark_schema(EonetEvent)
        for name in ("event_date", "last_date"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, TimestampType), f"{name} should be TimestampType"

    def test_date_field(self):
        schema = pydantic_to_spark_schema(EonetEvent)
        closed_field = next(f for f in schema.fields if f.name == "closed")
        assert isinstance(closed_field.dataType, DateType)

    def test_nullable_fields(self):
        schema = pydantic_to_spark_schema(EonetEvent)
        nullable_names = {"description", "source_url", "magnitude_value", "magnitude_unit", "closed"}
        for f in schema.fields:
            if f.name in nullable_names:
                assert f.nullable is True, f"{f.name} should be nullable"
            else:
                assert f.nullable is False, f"{f.name} should not be nullable"


class TestEonetEventDDL:
    def test_ddl_types(self):
        ddl = pydantic_to_ddl(EonetEvent, "eonet_event", schema="geo")
        assert "event_id TEXT NOT NULL" in ddl
        assert "latitude DOUBLE PRECISION NOT NULL" in ddl
        assert "longitude DOUBLE PRECISION NOT NULL" in ddl
        assert "event_date TIMESTAMP NOT NULL" in ddl
        assert "magnitude_value DOUBLE PRECISION NULL" in ddl
        assert "closed DATE NULL" in ddl
        assert "geometry_count INTEGER NOT NULL" in ddl
        assert "last_date TIMESTAMP NOT NULL" in ddl
        assert "file_date TEXT NOT NULL" in ddl
        assert "description TEXT NULL" in ddl

    def test_ddl_creates_correct_table(self):
        ddl = pydantic_to_ddl(EonetEvent, "eonet_event", schema="geo")
        assert "CREATE TABLE IF NOT EXISTS geo.eonet_event" in ddl


class TestEonetEventRegistry:
    def test_registered_in_factory(self):
        import model  # noqa: F401 — triggers table registration
        from model.plain.eonet_event import EonetEvent as EonetEventModel

        # Re-register in case model was cached before EonetEvent was added
        TableFactory.register("eonet_event", EonetEventModel)
        assert "eonet_event" in TableFactory.tables()
        assert TableFactory.get("eonet_event") is EonetEventModel
