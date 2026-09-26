import pytest
from pydantic import ValidationError
from pyspark.sql.types import StringType

from model.raw.usgs_volcano_raw import UsgsVolcanoRaw
from utils.schema_converter import pydantic_to_spark_schema

VALID_ROW = {
    "volcano_name": "Ahyi Seamount",
    "vnum": "284141",
    "observatory": "nmi",
    "observatory_name": "Northern Mariana Islands",
    "alert_level": "ADVISORY",
    "color_code": "YELLOW",
    "notice_type": "WU",
    "notice_identifier": "DOI-USGS-NMI-2026-09-10T23:17:05+00:00",
    "notice_date": "2026-09-10 23:25:53",
    "notice_url": "https://volcanoes.usgs.gov/hans-public/notice/DOI-USGS-NMI-2026-09-10T23:17:05+00:00",
    "notice_data_url": "https://volcanoes.usgs.gov/hans-public/api/notice/getNotice/DOI-USGS-NMI-2026-09-10T23:17:05+00:00",
    "file_date": "2026-09-09",
}


class TestUsgsVolcanoRawValid:
    def test_valid_row(self):
        model = UsgsVolcanoRaw(**VALID_ROW)
        assert model.volcano_name == "Ahyi Seamount"
        assert model.vnum == "284141"
        assert model.file_date == "2026-09-09"

    def test_optional_fields_none(self):
        row = dict.fromkeys(VALID_ROW)
        row["file_date"] = "2026-09-09"
        model = UsgsVolcanoRaw(**row)
        assert model.volcano_name is None
        assert model.vnum is None

    def test_all_12_fields_accepted(self):
        model = UsgsVolcanoRaw(**VALID_ROW)
        for name in VALID_ROW:
            assert getattr(model, name) == VALID_ROW[name]


class TestUsgsVolcanoRawMissing:
    def test_missing_file_date_raises(self):
        row = {k: v for k, v in VALID_ROW.items() if k != "file_date"}
        with pytest.raises(ValidationError):
            UsgsVolcanoRaw(**row)


class TestUsgsVolcanoRawSparkSchema:
    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(UsgsVolcanoRaw)
        assert len(schema.fields) == 12

    def test_all_fields_are_string_type(self):
        schema = pydantic_to_spark_schema(UsgsVolcanoRaw)
        for field in schema.fields:
            assert isinstance(field.dataType, StringType), f"{field.name} is not StringType"
