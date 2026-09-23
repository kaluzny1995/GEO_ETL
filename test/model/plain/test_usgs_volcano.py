import datetime as dt

import pytest

from model.plain.usgs_volcano import UsgsVolcano
from factory import TableFactory
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema
from pyspark.sql.types import (
    LongType,
    StringType,
    TimestampType,
)


class TestUsgsVolcanoValid:
    def test_valid_row(self):
        row = UsgsVolcano(
            volcano_name="Ahyi Seamount",
            vnum=284141,
            observatory="nmi",
            observatory_name="Northern Mariana Islands",
            alert_level="ADVISORY",
            color_code="YELLOW",
            notice_type="WU",
            notice_identifier="DOI-USGS-NMI-2026-09-10T23:17:05+00:00",
            notice_date=dt.datetime(2026, 9, 10, 23, 25, 53),
            notice_url="https://volcanoes.usgs.gov/hans-public/notice/DOI-USGS-NMI-2026-09-10T23:17:05+00:00",
            notice_data_url="https://volcanoes.usgs.gov/hans-public/api/notice/getNotice/DOI-USGS-NMI-2026-09-10T23:17:05+00:00",
            file_date="2026-09-09",
        )
        assert row.volcano_name == "Ahyi Seamount"
        assert row.vnum == 284141
        assert row.file_date == "2026-09-09"

    def test_optional_fields_none(self):
        row = UsgsVolcano(file_date="2026-09-09")
        assert row.volcano_name is None
        assert row.vnum is None


class TestUsgsVolcanoMissing:
    def test_missing_file_date_raises(self):
        with pytest.raises(Exception):
            UsgsVolcano(volcano_name="test")


class TestUsgsVolcanoSparkSchema:
    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(UsgsVolcano)
        assert len(schema.fields) == 12

    def test_int_field(self):
        schema = pydantic_to_spark_schema(UsgsVolcano)
        vnum = next(f for f in schema.fields if f.name == "vnum")
        assert isinstance(vnum.dataType, LongType)

    def test_timestamp_field(self):
        schema = pydantic_to_spark_schema(UsgsVolcano)
        notice_date = next(f for f in schema.fields if f.name == "notice_date")
        assert isinstance(notice_date.dataType, TimestampType)


class TestUsgsVolcanoDDL:
    def test_ddl_types(self):
        ddl = pydantic_to_ddl(UsgsVolcano, "usgs_volcano", schema="geo")
        assert "vnum INTEGER NULL" in ddl
        assert "notice_date TIMESTAMP NULL" in ddl
        assert "file_date TEXT NOT NULL" in ddl

    def test_ddl_creates_correct_table(self):
        ddl = pydantic_to_ddl(UsgsVolcano, "usgs_volcano", schema="geo")
        assert "CREATE TABLE IF NOT EXISTS geo.usgs_volcano" in ddl


class TestUsgsVolcanoRegistry:
    def test_registered_in_factory(self):
        import model  # noqa: F401
        from model.plain.usgs_volcano import UsgsVolcano as UsgsVolcanoModel
        TableFactory.register("usgs_volcano", UsgsVolcanoModel)
        assert "usgs_volcano" in TableFactory.tables()
        assert TableFactory.get("usgs_volcano") is UsgsVolcanoModel
