import datetime as dt

import pytest
from pydantic import ValidationError
from pyspark.sql.types import (
    DoubleType,
    StringType,
    TimestampType,
)

from factory import TableFactory
from model.plain.nifc_fire import NifcFire
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema


class TestNifcFireValid:
    def test_valid_row(self):
        row = NifcFire(
            incident_name="Silvertip",
            irwin_id="{C4138756-16B2-412F-B32B-4DB4520DE5BC}",
            fire_number=None,
            acres=123.4,
            percent_contained=50.0,
            fire_type="WF",
            fire_type_label="Wildfire Daily Fire Perimeter",
            jurisdiction="USFS",
            jurisdiction_label="WAOWF",
            unit_name="Okanogan-Wenatchee National Forest",
            state="MT",
            po_method=None,
            latitude=47.843,
            longitude=-113.240,
            poly_date=dt.datetime(2026, 9, 1, 0, 0, 0),
            created_on=dt.datetime(2026, 8, 1, 0, 0, 0),
            modified_on=dt.datetime(2026, 9, 1, 0, 0, 0),
            file_date="2026-09-01",
        )
        assert row.incident_name == "Silvertip"
        assert row.acres == 123.4
        assert row.file_date == "2026-09-01"

    def test_optional_fields_none(self):
        row = NifcFire(
            incident_name="Test",
            file_date="2026-09-01",
        )
        assert row.irwin_id is None
        assert row.acres is None
        assert row.latitude is None


class TestNifcFireMissing:
    def test_missing_file_date_raises(self):
        with pytest.raises(ValidationError):
            NifcFire(incident_name="test")


class TestNifcFireSparkSchema:
    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(NifcFire)
        assert len(schema.fields) == 19

    def test_double_fields(self):
        schema = pydantic_to_spark_schema(NifcFire)
        double_fields = {"acres", "percent_contained", "latitude", "longitude"}
        for f in schema.fields:
            if f.name in double_fields:
                assert isinstance(f.dataType, DoubleType), f"{f.name} should be DoubleType"

    def test_str_fields(self):
        schema = pydantic_to_spark_schema(NifcFire)
        str_fields = {"fire_type", "jurisdiction"}
        for f in schema.fields:
            if f.name in str_fields:
                assert isinstance(f.dataType, StringType), f"{f.name} should be StringType"

    def test_timestamp_fields(self):
        schema = pydantic_to_spark_schema(NifcFire)
        for name in ("poly_date", "created_on", "modified_on"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, TimestampType), f"{name} should be TimestampType"


class TestNifcFireDDL:
    def test_ddl_types(self):
        ddl = pydantic_to_ddl(NifcFire, "nifc_fire", schema="geo")
        assert "acres DOUBLE PRECISION NULL" in ddl
        assert "fire_type TEXT NULL" in ddl
        assert "jurisdiction TEXT NULL" in ddl
        assert "unit_name TEXT NULL" in ddl
        assert "poly_date TIMESTAMP NULL" in ddl
        assert "file_date TEXT NOT NULL" in ddl

    def test_ddl_creates_correct_table(self):
        ddl = pydantic_to_ddl(NifcFire, "nifc_fire", schema="geo")
        assert "CREATE TABLE IF NOT EXISTS geo.nifc_fire" in ddl


class TestNifcFireRegistry:
    def test_registered_in_factory(self):
        import model  # noqa: F401
        from model.plain.nifc_fire import NifcFire as NifcFireModel

        TableFactory.register("nifc_fire", NifcFireModel)
        assert "nifc_fire" in TableFactory.tables()
        assert TableFactory.get("nifc_fire") is NifcFireModel
