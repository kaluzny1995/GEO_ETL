import pytest
from pydantic import ValidationError
from pyspark.sql.types import StringType

from model.raw.usgs_nwis_raw import UsgsNwisRaw
from utils.schema_converter import pydantic_to_spark_schema

VALID_ROW = {
    "site_id": "USGS-01646500",
    "site_name": "POTOMAC RIVER NEAR WASH, DC LITTLE FALLS PUMP STA",
    "parameter_code": "00060",
    "date": "2026-09-01",
    "value": "2320",
    "unit": "ft3/s",
    "qualifiers": "P",
    "latitude": "38.94977778",
    "longitude": "-77.12763889",
    "file_date": "2026-09-01",
}


class TestUsgsNwisRawValid:
    def test_valid_row(self):
        model = UsgsNwisRaw(**VALID_ROW)
        assert model.site_id == "USGS-01646500"
        assert model.value == "2320"
        assert model.file_date == "2026-09-01"

    def test_optional_fields_none(self):
        row = dict.fromkeys(VALID_ROW)
        row["file_date"] = "2026-09-01"
        model = UsgsNwisRaw(**row)
        assert model.site_id is None
        assert model.value is None

    def test_all_10_fields_accepted(self):
        model = UsgsNwisRaw(**VALID_ROW)
        for name in VALID_ROW:
            assert getattr(model, name) == VALID_ROW[name]


class TestUsgsNwisRawMissing:
    def test_missing_file_date_raises(self):
        row = {k: v for k, v in VALID_ROW.items() if k != "file_date"}
        with pytest.raises(ValidationError):
            UsgsNwisRaw(**row)


class TestUsgsNwisRawSparkSchema:
    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(UsgsNwisRaw)
        assert len(schema.fields) == 10

    def test_all_fields_are_string_type(self):
        schema = pydantic_to_spark_schema(UsgsNwisRaw)
        for field in schema.fields:
            assert isinstance(field.dataType, StringType), f"{field.name} is not StringType"
