import datetime as dt

import pytest
from pydantic import ValidationError
from pyspark.sql.types import (
    DateType,
    DoubleType,
)

from factory import TableFactory
from model.plain.usgs_nwis import UsgsNwis
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema


class TestUsgsNwisValid:
    def test_valid_row(self):
        row = UsgsNwis(
            site_id="USGS-01646500",
            site_name="POTOMAC RIVER NEAR WASH, DC",
            parameter_code="00060",
            date=dt.date(2026, 9, 1),
            value=2320.0,
            unit="ft3/s",
            qualifiers="P",
            latitude=38.949,
            longitude=-77.127,
            file_date="2026-09-01",
        )
        assert row.site_id == "USGS-01646500"
        assert row.value == 2320.0
        assert row.file_date == "2026-09-01"

    def test_optional_fields_none(self):
        row = UsgsNwis(file_date="2026-09-01")
        assert row.site_id is None
        assert row.value is None


class TestUsgsNwisMissing:
    def test_missing_file_date_raises(self):
        with pytest.raises(ValidationError):
            UsgsNwis(site_id="test")


class TestUsgsNwisSparkSchema:
    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(UsgsNwis)
        assert len(schema.fields) == 10

    def test_double_fields(self):
        schema = pydantic_to_spark_schema(UsgsNwis)
        double_fields = {"value", "latitude", "longitude"}
        for f in schema.fields:
            if f.name in double_fields:
                assert isinstance(f.dataType, DoubleType), f"{f.name} should be DoubleType"

    def test_date_field(self):
        schema = pydantic_to_spark_schema(UsgsNwis)
        date_field = next(f for f in schema.fields if f.name == "date")
        assert isinstance(date_field.dataType, DateType)


class TestUsgsNwisDDL:
    def test_ddl_types(self):
        ddl = pydantic_to_ddl(UsgsNwis, "usgs_nwis", schema="geo")
        assert "value DOUBLE PRECISION NULL" in ddl
        assert "latitude DOUBLE PRECISION NULL" in ddl
        assert "date DATE NULL" in ddl
        assert "file_date TEXT NOT NULL" in ddl

    def test_ddl_creates_correct_table(self):
        ddl = pydantic_to_ddl(UsgsNwis, "usgs_nwis", schema="geo")
        assert "CREATE TABLE IF NOT EXISTS geo.usgs_nwis" in ddl


class TestUsgsNwisRegistry:
    def test_registered_in_factory(self):
        import model  # noqa: F401
        from model.plain.usgs_nwis import UsgsNwis as UsgsNwisModel

        TableFactory.register("usgs_nwis", UsgsNwisModel)
        assert "usgs_nwis" in TableFactory.tables()
        assert TableFactory.get("usgs_nwis") is UsgsNwisModel
