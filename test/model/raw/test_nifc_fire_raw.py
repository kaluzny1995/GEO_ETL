import pytest
from pydantic import ValidationError
from pyspark.sql.types import StringType

from model.raw.nifc_fire_raw import NifcFireRaw
from utils.schema_converter import pydantic_to_spark_schema

VALID_ROW = {
    "incident_name": "Silvertip",
    "irwin_id": "{C4138756-16B2-412F-B32B-4DB4520DE5BC}",
    "fire_number": "",
    "acres": "",
    "percent_contained": "",
    "fire_type": "",
    "fire_type_label": "",
    "jurisdiction": "",
    "jurisdiction_label": "",
    "unit_name": "",
    "state": "",
    "po_method": "",
    "latitude": "47.84370529048297",
    "longitude": "-113.24066952697544",
    "poly_date": "1788305461000",
    "created_on": "1784928492297",
    "modified_on": "1789662976887",
    "file_date": "2026-09-01",
}


# GEO_Download writes the CSV columns in exactly this order, and the upload step reads
# them with an explicit Spark schema (positional mapping), so reordering either side
# silently shifts values between columns. Keep in sync with downloader/nifc.py.
CSV_HEADER_ORDER = [
    "incident_name",
    "irwin_id",
    "fire_number",
    "acres",
    "percent_contained",
    "fire_type",
    "fire_type_label",
    "jurisdiction",
    "jurisdiction_label",
    "unit_name",
    "state",
    "po_method",
    "latitude",
    "longitude",
    "poly_date",
    "created_on",
    "modified_on",
]


class TestNifcFireRawValid:
    def test_valid_row(self):
        model = NifcFireRaw(**VALID_ROW)
        assert model.incident_name == "Silvertip"
        assert model.irwin_id == "{C4138756-16B2-412F-B32B-4DB4520DE5BC}"
        assert model.file_date == "2026-09-01"

    def test_optional_fields_none(self):
        row = dict.fromkeys(VALID_ROW)
        row["file_date"] = "2026-09-01"
        model = NifcFireRaw(**row)
        assert model.incident_name is None
        assert model.latitude is None

    def test_all_17_fields_accepted(self):
        model = NifcFireRaw(**VALID_ROW)
        for name in VALID_ROW:
            assert getattr(model, name) == VALID_ROW[name]


class TestNifcFireRawMissing:
    def test_missing_file_date_raises(self):
        row = {k: v for k, v in VALID_ROW.items() if k != "file_date"}
        with pytest.raises(ValidationError):
            NifcFireRaw(**row)


class TestNifcFireRawSparkSchema:
    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(NifcFireRaw)
        assert len(schema.fields) == 18

    def test_all_fields_are_string_type(self):
        schema = pydantic_to_spark_schema(NifcFireRaw)
        for field in schema.fields:
            assert isinstance(field.dataType, StringType), f"{field.name} is not StringType"

    def test_field_order_matches_download_csv_header(self):
        schema = pydantic_to_spark_schema(NifcFireRaw)
        csv_fields = [field.name for field in schema.fields if field.name != "file_date"]
        assert csv_fields == CSV_HEADER_ORDER
