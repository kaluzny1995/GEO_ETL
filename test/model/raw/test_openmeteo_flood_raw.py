import pytest
from pydantic import ValidationError
from pyspark.sql.types import StringType

from model.raw.openmeteo_flood_raw import OpenMeteoFloodRaw
from utils.schema_converter import pydantic_to_spark_schema

VALID_ROW = {
    "river_name": "Amazon",
    "country": "Brazil",
    "latitude": "-3.14",
    "longitude": "-59.98",
    "date": "2026-09-01",
    "discharge_m3s": "77982.05",
    "file_date": "2026-09-01",
}


class TestOpenMeteoFloodRawValid:
    def test_valid_row(self):
        model = OpenMeteoFloodRaw(**VALID_ROW)
        assert model.river_name == "Amazon"
        assert model.country == "Brazil"
        assert model.file_date == "2026-09-01"

    def test_optional_fields_none(self):
        row = dict.fromkeys(VALID_ROW)
        row["file_date"] = "2026-09-01"
        model = OpenMeteoFloodRaw(**row)
        assert model.river_name is None
        assert model.latitude is None

    def test_all_7_fields_accepted(self):
        model = OpenMeteoFloodRaw(**VALID_ROW)
        for name in VALID_ROW:
            assert getattr(model, name) == VALID_ROW[name]


class TestOpenMeteoFloodRawMissing:
    def test_missing_file_date_raises(self):
        row = {k: v for k, v in VALID_ROW.items() if k != "file_date"}
        with pytest.raises(ValidationError):
            OpenMeteoFloodRaw(**row)


class TestOpenMeteoFloodRawSparkSchema:
    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(OpenMeteoFloodRaw)
        assert len(schema.fields) == 9

    def test_all_fields_are_string_type(self):
        schema = pydantic_to_spark_schema(OpenMeteoFloodRaw)
        for field in schema.fields:
            assert isinstance(field.dataType, StringType), f"{field.name} is not StringType"
