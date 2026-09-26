import pytest
from pydantic import ValidationError
from pyspark.sql.types import StringType

from model.raw.gdacs_event_raw import GdacsEventRaw
from utils.schema_converter import pydantic_to_spark_schema

VALID_ROW = {
    "event_type": "WF",
    "event_id": "1031770",
    "event_name": "Forest fires in Australia",
    "alert_level": "Green",
    "country": "Australia",
    "country_code": "AUS",
    "latitude": "-15.7068",
    "longitude": "130.5705",
    "from_date": "2026-09-02T00:00:00",
    "to_date": "2026-09-17T00:00:00",
    "date_modified": "2026-09-17T19:35:40",
    "severity": "14196.0",
    "severity_text": "Green impact for forestfire in 14196 ha",
    "severity_unit": "ha",
    "source": "GWIS",
    "report_url": "https://www.gdacs.org/report.aspx?eventid=1031770&episodeid=27&eventtype=WF",
    "is_current": "true",
    "file_date": "2026-09-01",
}


class TestGdacsEventRawValid:
    def test_valid_row(self):
        model = GdacsEventRaw(**VALID_ROW)
        assert model.event_id == "1031770"
        assert model.file_date == "2026-09-01"

    def test_optional_fields_none(self):
        row = {**VALID_ROW, "country": None, "country_code": None, "severity_text": None, "severity_unit": None}
        model = GdacsEventRaw(**row)
        assert model.country is None
        assert model.severity_text is None

    def test_optional_fields_present(self):
        model = GdacsEventRaw(**VALID_ROW)
        assert model.country == "Australia"
        assert model.severity_unit == "ha"


class TestGdacsEventRawMissing:
    def test_missing_required_field_raises(self):
        row = {k: v for k, v in VALID_ROW.items() if k != "event_id"}
        with pytest.raises(ValidationError):
            GdacsEventRaw(**row)

    def test_missing_file_date_raises(self):
        row = {k: v for k, v in VALID_ROW.items() if k != "file_date"}
        with pytest.raises(ValidationError):
            GdacsEventRaw(**row)


class TestGdacsEventRawSparkSchema:
    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(GdacsEventRaw)
        assert len(schema.fields) == 18

    def test_schema_field_names(self):
        schema = pydantic_to_spark_schema(GdacsEventRaw)
        names = [f.name for f in schema.fields]
        assert names == [
            "event_type",
            "event_id",
            "event_name",
            "alert_level",
            "country",
            "country_code",
            "latitude",
            "longitude",
            "from_date",
            "to_date",
            "date_modified",
            "severity",
            "severity_text",
            "severity_unit",
            "source",
            "report_url",
            "is_current",
            "file_date",
        ]

    def test_all_fields_are_string_type(self):
        schema = pydantic_to_spark_schema(GdacsEventRaw)
        for field in schema.fields:
            assert isinstance(field.dataType, StringType), f"{field.name} is not StringType"

    def test_nullable_fields(self):
        schema = pydantic_to_spark_schema(GdacsEventRaw)
        nullable_fields = {f.name for f in schema.fields if f.nullable}
        assert nullable_fields == {"country", "country_code", "severity_text", "severity_unit"}
