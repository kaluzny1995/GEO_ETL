import datetime as dt

import pytest
from pydantic import ValidationError
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    TimestampType,
)

from factory import TableFactory
from model.other.data_quality import DataQuality
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema


class TestDataQualityValid:
    def test_valid_row(self):
        row = DataQuality(
            table_name="avalanche_org",
            file_date="2026-09-20",
            metric="total_records",
            value=120.0,
            computed_at=dt.datetime(2026, 9, 20, 12, 0, 0),
        )
        assert row.table_name == "avalanche_org"
        assert row.metric == "total_records"
        assert row.value == 120.0

    def test_value_coerced_to_float(self):
        row = DataQuality(
            table_name="avalanche_org",
            file_date="2026-09-20",
            metric="bad_row_pct",
            value=1,
            computed_at=dt.datetime(2026, 9, 20, 12, 0, 0),
        )
        assert isinstance(row.value, float)
        assert row.value == 1.0

    def test_zero_value(self):
        row = DataQuality(
            table_name="usgs_earthquake",
            file_date="2026-09-20",
            metric="null_pct__magnitude",
            value=0.0,
            computed_at=dt.datetime(2026, 9, 20, 12, 0, 0),
        )
        assert row.value == 0.0


class TestDataQualityMissing:
    def test_missing_required_field_raises(self):
        with pytest.raises(ValidationError):
            DataQuality(table_name="avalanche_org")

    def test_missing_value_raises(self):
        with pytest.raises(ValidationError):
            DataQuality(
                table_name="avalanche_org",
                file_date="2026-09-20",
                metric="total_records",
                computed_at=dt.datetime(2026, 9, 20, 12, 0, 0),
            )


class TestDataQualitySparkSchema:
    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(DataQuality)
        assert len(schema.fields) == 5

    def test_schema_field_names(self):
        schema = pydantic_to_spark_schema(DataQuality)
        names = [f.name for f in schema.fields]
        assert names == ["table_name", "file_date", "metric", "value", "computed_at"]

    def test_text_fields(self):
        schema = pydantic_to_spark_schema(DataQuality)
        for name in ("table_name", "file_date", "metric"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, StringType)
            assert field.nullable is False

    def test_value_field_is_double(self):
        schema = pydantic_to_spark_schema(DataQuality)
        field = next(f for f in schema.fields if f.name == "value")
        assert isinstance(field.dataType, DoubleType)

    def test_computed_at_field_is_timestamp(self):
        schema = pydantic_to_spark_schema(DataQuality)
        field = next(f for f in schema.fields if f.name == "computed_at")
        assert isinstance(field.dataType, TimestampType)


class TestDataQualityDDL:
    def test_ddl_types(self):
        ddl = pydantic_to_ddl(DataQuality, "data_quality", schema="geo")
        assert "table_name TEXT NOT NULL" in ddl
        assert "file_date TEXT NOT NULL" in ddl
        assert "metric TEXT NOT NULL" in ddl
        assert "value DOUBLE PRECISION NOT NULL" in ddl
        assert "computed_at TIMESTAMP NOT NULL" in ddl

    def test_ddl_creates_correct_table(self):
        ddl = pydantic_to_ddl(DataQuality, "data_quality", schema="geo")
        assert "CREATE TABLE IF NOT EXISTS geo.data_quality" in ddl


class TestDataQualityRegistry:
    def test_registered_in_factory(self):
        TableFactory.register("data_quality", DataQuality)
        assert "data_quality" in TableFactory.tables()
        assert TableFactory.get("data_quality") is DataQuality


def test_no_long_type_in_schema():
    schema = pydantic_to_spark_schema(DataQuality)
    assert not any(isinstance(f.dataType, LongType) for f in schema.fields)
