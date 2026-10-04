import pytest
from pydantic import ValidationError
from pyspark.sql.types import StringType

from model.raw.usgs_earthquake_raw import UsgsEarthquakeRaw
from utils.schema_converter import pydantic_to_spark_schema

VALID_ROW = {
    "time": "2026-09-01T23:58:46.050Z",
    "latitude": "33.704166666667",
    "longitude": "-116.80866666667",
    "depth": "15.95",
    "mag": "0.52",
    "magType": "ml",
    "nst": "15",
    "gap": "149",
    "dmin": "0.07908",
    "rms": "0.08",
    "net": "ci",
    "id": "ci41540448",
    "updated": "2026-09-04T19:13:03.582Z",
    "place": "9 km WSW of Idyllwild, CA",
    "type": "earthquake",
    "horizontalError": "0.29",
    "depthError": "0.48",
    "magError": "0.07743263870696",
    "magNst": "6",
    "status": "reviewed",
    "locationSource": "ci",
    "magSource": "ci",
    "file_date": "2026-09-01",
}


class TestUsgsEarthquakeRawValid:
    def test_valid_row(self):
        model = UsgsEarthquakeRaw(**VALID_ROW)
        assert model.time == "2026-09-01T23:58:46.050Z"
        assert model.id == "ci41540448"
        assert model.file_date == "2026-09-01"

    def test_optional_fields_none(self):
        row = dict.fromkeys(VALID_ROW)
        row["file_date"] = "2026-09-01"
        model = UsgsEarthquakeRaw(**row)
        assert model.time is None
        assert model.place is None

    def test_all_23_fields_accepted(self):
        model = UsgsEarthquakeRaw(**VALID_ROW)
        for name in VALID_ROW:
            assert getattr(model, name) == VALID_ROW[name]


class TestUsgsEarthquakeRawMissing:
    def test_missing_file_date_raises(self):
        row = {k: v for k, v in VALID_ROW.items() if k != "file_date"}
        with pytest.raises(ValidationError):
            UsgsEarthquakeRaw(**row)


class TestUsgsEarthquakeRawSparkSchema:
    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(UsgsEarthquakeRaw)
        assert len(schema.fields) == 25

    def test_all_fields_are_string_type(self):
        schema = pydantic_to_spark_schema(UsgsEarthquakeRaw)
        for field in schema.fields:
            assert isinstance(field.dataType, StringType), f"{field.name} is not StringType"
