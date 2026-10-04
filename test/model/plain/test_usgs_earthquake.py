import datetime as dt

import pytest
from pydantic import ValidationError
from pyspark.sql.types import (
    DoubleType,
    LongType,
    TimestampType,
)

from factory import TableFactory
from model.plain.usgs_earthquake import UsgsEarthquake
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema


class TestUsgsEarthquakeValid:
    def test_valid_row(self):
        row = UsgsEarthquake(
            time=dt.datetime(2026, 9, 1, 23, 58, 46),
            latitude=33.704,
            longitude=-116.808,
            depth=15.95,
            mag=0.52,
            mag_type="ml",
            nst=15,
            gap=149,
            dmin=0.07908,
            rms=0.08,
            net="ci",
            event_id="ci41540448",
            updated=dt.datetime(2026, 9, 4, 19, 13, 3),
            place="9 km WSW of Idyllwild, CA",
            event_type="earthquake",
            horizontal_error=0.29,
            depth_error=0.48,
            mag_error=0.077,
            mag_nst=6,
            status="reviewed",
            location_source="ci",
            mag_source="ci",
            file_date="2026-09-01",
        )
        assert row.event_id == "ci41540448"
        assert row.mag == 0.52
        assert row.file_date == "2026-09-01"

    def test_optional_fields_none(self):
        row = UsgsEarthquake(
            file_date="2026-09-01",
        )
        assert row.time is None
        assert row.latitude is None
        assert row.mag is None


class TestUsgsEarthquakeMissing:
    def test_missing_file_date_raises(self):
        with pytest.raises(ValidationError):
            UsgsEarthquake(event_id="test")


class TestUsgsEarthquakeSparkSchema:
    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(UsgsEarthquake)
        assert len(schema.fields) == 25

    def test_double_fields(self):
        schema = pydantic_to_spark_schema(UsgsEarthquake)
        double_fields = {
            "latitude",
            "longitude",
            "depth",
            "mag",
            "dmin",
            "rms",
            "horizontal_error",
            "depth_error",
            "mag_error",
        }
        for f in schema.fields:
            if f.name in double_fields:
                assert isinstance(f.dataType, DoubleType), f"{f.name} should be DoubleType"

    def test_int_fields(self):
        schema = pydantic_to_spark_schema(UsgsEarthquake)
        int_fields = {"nst", "gap", "mag_nst"}
        for f in schema.fields:
            if f.name in int_fields:
                assert isinstance(f.dataType, LongType), f"{f.name} should be LongType"

    def test_timestamp_fields(self):
        schema = pydantic_to_spark_schema(UsgsEarthquake)
        for name in ("time", "updated"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, TimestampType), f"{name} should be TimestampType"


class TestUsgsEarthquakeDDL:
    def test_ddl_types(self):
        ddl = pydantic_to_ddl(UsgsEarthquake, "usgs_earthquake", schema="geo")
        assert "latitude DOUBLE PRECISION NULL" in ddl
        assert "depth DOUBLE PRECISION NULL" in ddl
        assert "mag DOUBLE PRECISION NULL" in ddl
        assert "nst INTEGER NULL" in ddl
        assert "time TIMESTAMP NULL" in ddl
        assert "event_id TEXT NULL" in ddl
        assert "file_date TEXT NOT NULL" in ddl

    def test_ddl_creates_correct_table(self):
        ddl = pydantic_to_ddl(UsgsEarthquake, "usgs_earthquake", schema="geo")
        assert "CREATE TABLE IF NOT EXISTS geo.usgs_earthquake" in ddl


class TestUsgsEarthquakeRegistry:
    def test_registered_in_factory(self):
        import model  # noqa: F401
        from model.plain.usgs_earthquake import UsgsEarthquake as UsgsEarthquakeModel

        TableFactory.register("usgs_earthquake", UsgsEarthquakeModel)
        assert "usgs_earthquake" in TableFactory.tables()
        assert TableFactory.get("usgs_earthquake") is UsgsEarthquakeModel
