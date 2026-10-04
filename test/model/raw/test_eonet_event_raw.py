import pytest
from pydantic import ValidationError
from pyspark.sql.types import StringType

from model.raw.eonet_event_raw import EonetEventRaw
from utils.schema_converter import pydantic_to_spark_schema

VALID_ROW = {
    "event_id": "EONET_23870",
    "event_name": "Wildfire Snow, Custer, Montana",
    "category_id": "wildfires",
    "category_name": "Wildfires",
    "description": "30 Miles E from Miles City, MT",
    "source_id": "IRWIN",
    "source_url": "https://irwin.doi.gov/observer/incidents/2026-MTLG14-000775",
    "latitude": "46.48575",
    "longitude": "-105.185017",
    "event_date": "2026-09-02T22:16:00Z",
    "magnitude_value": "1100.0",
    "magnitude_unit": "acres",
    "closed": None,
    "geometry_count": "1",
    "last_latitude": "46.48575",
    "last_longitude": "-105.185017",
    "last_date": "2026-09-02T22:16:00Z",
    "file_date": "2026-09-01",
}


class TestEonetEventRawValid:
    def test_valid_row(self):
        model = EonetEventRaw(**VALID_ROW)
        assert model.event_id == "EONET_23870"
        assert model.file_date == "2026-09-01"

    def test_optional_fields_none(self):
        row = {
            **VALID_ROW,
            "description": None,
            "source_url": None,
            "magnitude_value": None,
            "magnitude_unit": None,
            "closed": None,
        }
        model = EonetEventRaw(**row)
        assert model.description is None
        assert model.source_url is None
        assert model.magnitude_value is None

    def test_optional_fields_present(self):
        model = EonetEventRaw(**VALID_ROW)
        assert model.magnitude_value == "1100.0"
        assert model.magnitude_unit == "acres"


class TestEonetEventRawMissing:
    def test_missing_required_field_raises(self):
        row = {k: v for k, v in VALID_ROW.items() if k != "event_id"}
        with pytest.raises(ValidationError):
            EonetEventRaw(**row)

    def test_missing_file_date_raises(self):
        row = {k: v for k, v in VALID_ROW.items() if k != "file_date"}
        with pytest.raises(ValidationError):
            EonetEventRaw(**row)


class TestEonetEventRawSparkSchema:
    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(EonetEventRaw)
        assert len(schema.fields) == 20

    def test_schema_field_names(self):
        schema = pydantic_to_spark_schema(EonetEventRaw)
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
            "geocode_country",
            "geocode_state",
            "file_date",
        ]

    def test_all_fields_are_string_type(self):
        schema = pydantic_to_spark_schema(EonetEventRaw)
        for field in schema.fields:
            assert isinstance(field.dataType, StringType), f"{field.name} is not StringType"

    def test_nullable_fields(self):
        schema = pydantic_to_spark_schema(EonetEventRaw)
        nullable_fields = {f.name for f in schema.fields if f.nullable}
        assert nullable_fields == {
            "description",
            "source_url",
            "magnitude_value",
            "magnitude_unit",
            "closed",
            "geocode_country",
            "geocode_state",
        }
