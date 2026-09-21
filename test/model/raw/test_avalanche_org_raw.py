import pytest
from pydantic import ValidationError

from model.raw.avalanche_org_raw import AvalancheOrgRaw
from utils.schema_converter import pydantic_to_spark_schema
from pyspark.sql.types import StringType

VALID_ROW = {
    "zone_id": "3004",
    "zone_name": "Bridgeport Avalanche Center",
    "center_id": "BAC",
    "center_name": "Bridgeport Avalanche Center",
    "state": "CA",
    "danger_level": "-1",
    "danger_rating": "no rating",
    "travel_advice": "Watch for signs of unstable snow.",
    "off_season": "False",
    "forecast_start": "2026-04-13T14:00:00",
    "forecast_end": "2026-11-01T14:30:00",
    "warning": None,
    "link": "https://bridgeportavalanchecenter.org/avalanche-forecast/",
    "date": "2026-09-01",
    "file_date": "2026-09-01",
}


class TestAvalancheOrgRawValid:
    def test_valid_row(self):
        model = AvalancheOrgRaw(**VALID_ROW)
        assert model.zone_id == "3004"
        assert model.file_date == "2026-09-01"

    def test_optional_fields_none(self):
        row = {**VALID_ROW, "forecast_start": None, "forecast_end": None, "warning": None}
        model = AvalancheOrgRaw(**row)
        assert model.forecast_start is None
        assert model.forecast_end is None
        assert model.warning is None

    def test_optional_fields_present(self):
        model = AvalancheOrgRaw(**VALID_ROW)
        assert model.forecast_start == "2026-04-13T14:00:00"
        assert model.forecast_end == "2026-11-01T14:30:00"


class TestAvalancheOrgRawMissing:
    def test_missing_required_field_raises(self):
        row = {k: v for k, v in VALID_ROW.items() if k != "zone_id"}
        with pytest.raises(ValidationError):
            AvalancheOrgRaw(**row)

    def test_missing_file_date_raises(self):
        row = {k: v for k, v in VALID_ROW.items() if k != "file_date"}
        with pytest.raises(ValidationError):
            AvalancheOrgRaw(**row)


class TestAvalancheOrgRawSparkSchema:
    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(AvalancheOrgRaw)
        assert len(schema.fields) == 15

    def test_schema_field_names(self):
        schema = pydantic_to_spark_schema(AvalancheOrgRaw)
        names = [f.name for f in schema.fields]
        assert names == [
            "zone_id", "zone_name", "center_id", "center_name", "state",
            "danger_level", "danger_rating", "travel_advice", "off_season",
            "forecast_start", "forecast_end", "warning", "link", "date",
            "file_date",
        ]

    def test_all_fields_are_string_type(self):
        schema = pydantic_to_spark_schema(AvalancheOrgRaw)
        for field in schema.fields:
            assert isinstance(field.dataType, StringType), f"{field.name} is not StringType"

    def test_nullable_fields(self):
        schema = pydantic_to_spark_schema(AvalancheOrgRaw)
        nullable_fields = {f.name for f in schema.fields if f.nullable}
        assert nullable_fields == {"forecast_start", "forecast_end", "warning"}
