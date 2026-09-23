import datetime as dt

import pytest

from model.plain.openmeteo_flood import OpenMeteoFlood
from factory import TableFactory
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema
from pyspark.sql.types import (
    DateType,
    DoubleType,
    StringType,
)


class TestOpenMeteoFloodValid:
    def test_valid_row(self):
        row = OpenMeteoFlood(
            river_name="Amazon",
            country="Brazil",
            latitude=-3.14,
            longitude=-59.98,
            date=dt.date(2026, 9, 1),
            discharge_m3s=77982.05,
            file_date="2026-09-01",
        )
        assert row.river_name == "Amazon"
        assert row.discharge_m3s == 77982.05
        assert row.file_date == "2026-09-01"

    def test_optional_fields_none(self):
        row = OpenMeteoFlood(
            file_date="2026-09-01",
        )
        assert row.river_name is None
        assert row.latitude is None
        assert row.discharge_m3s is None


class TestOpenMeteoFloodMissing:
    def test_missing_file_date_raises(self):
        with pytest.raises(Exception):
            OpenMeteoFlood(river_name="test")


class TestOpenMeteoFloodSparkSchema:
    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(OpenMeteoFlood)
        assert len(schema.fields) == 7

    def test_double_fields(self):
        schema = pydantic_to_spark_schema(OpenMeteoFlood)
        double_fields = {"latitude", "longitude", "discharge_m3s"}
        for f in schema.fields:
            if f.name in double_fields:
                assert isinstance(f.dataType, DoubleType), f"{f.name} should be DoubleType"

    def test_date_field(self):
        schema = pydantic_to_spark_schema(OpenMeteoFlood)
        date_field = next(f for f in schema.fields if f.name == "date")
        assert isinstance(date_field.dataType, DateType)


class TestOpenMeteoFloodDDL:
    def test_ddl_types(self):
        ddl = pydantic_to_ddl(OpenMeteoFlood, "openmeteo_flood", schema="geo")
        assert "latitude DOUBLE PRECISION NULL" in ddl
        assert "longitude DOUBLE PRECISION NULL" in ddl
        assert "discharge_m3s DOUBLE PRECISION NULL" in ddl
        assert "date DATE NULL" in ddl
        assert "file_date TEXT NOT NULL" in ddl

    def test_ddl_creates_correct_table(self):
        ddl = pydantic_to_ddl(OpenMeteoFlood, "openmeteo_flood", schema="geo")
        assert "CREATE TABLE IF NOT EXISTS geo.openmeteo_flood" in ddl


class TestOpenMeteoFloodRegistry:
    def test_registered_in_factory(self):
        import model  # noqa: F401
        from model.plain.openmeteo_flood import OpenMeteoFlood as OpenMeteoFloodModel
        TableFactory.register("openmeteo_flood", OpenMeteoFloodModel)
        assert "openmeteo_flood" in TableFactory.tables()
        assert TableFactory.get("openmeteo_flood") is OpenMeteoFloodModel
