import datetime as dt

import pytest
from pydantic import ValidationError
from pyspark.sql.types import (
    DoubleType,
    LongType,
    TimestampType,
)

from factory import TableFactory
from model.plain.ibtracs_cyclone import IbtacsCyclone
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema


class TestIbtacsCycloneValid:
    def test_valid_row(self):
        row = IbtacsCyclone(
            sid="2026237N09256",
            season=2026,
            number=57,
            basin="EP",
            subbasin="MM",
            name="KARINA",
            iso_time=dt.datetime(2026, 9, 1, 0, 0, 0),
            nature="TS",
            lat=17.7,
            lon=-126.3,
            wmo_wind=None,
            wmo_pres=None,
            wmo_agency=None,
            track_type="PROVISIONAL",
            dist2land=1608,
            landfall=1608,
            iflag="O______________",
            usa_agency="nhc_working_bt",
            usa_atcf_id="EP112026",
            usa_lat=17.7,
            usa_lon=-126.3,
            usa_record=None,
            usa_status="HU",
            usa_wind=115,
            usa_pres=950,
            usa_sshs=4,
            usa_r34_ne=100,
            usa_r34_se=80,
            usa_r34_sw=70,
            usa_r34_nw=100,
            usa_r50_ne=50,
            usa_r50_se=35,
            usa_r50_sw=30,
            usa_r50_nw=50,
            usa_r64_ne=25,
            usa_r64_se=15,
            usa_r64_sw=15,
            usa_r64_nw=25,
            usa_poci=1007,
            usa_roci=250,
            usa_rmw=15,
            usa_eye=None,
            storm_speed=270,
            storm_dir=10,
            usa_gust=140,
            file_date="2026-09-01",
        )
        assert row.sid == "2026237N09256"
        assert row.season == 2026
        assert row.usa_wind == 115
        assert row.file_date == "2026-09-01"

    def test_optional_fields_none(self):
        row = IbtacsCyclone(
            sid="2026237N09256",
            season=2026,
            number=57,
            basin="EP",
            iso_time=dt.datetime(2026, 9, 1, 0, 0, 0),
            lat=17.7,
            lon=-126.3,
            file_date="2026-09-01",
        )
        assert row.name is None
        assert row.usa_wind is None
        assert row.usa_gust is None


class TestIbtacsCycloneMissing:
    def test_missing_required_field_raises(self):
        with pytest.raises(ValidationError):
            IbtacsCyclone(name="test")

    def test_missing_sid_raises(self):
        with pytest.raises(ValidationError):
            IbtacsCyclone(
                season=2026,
                number=57,
                basin="EP",
                iso_time=dt.datetime(2026, 9, 1),
                lat=17.7,
                lon=-126.3,
                file_date="2026-09-01",
            )


class TestIbtacsCycloneSparkSchema:
    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(IbtacsCyclone)
        assert len(schema.fields) == 48

    def test_schema_field_names(self):
        schema = pydantic_to_spark_schema(IbtacsCyclone)
        names = [f.name for f in schema.fields]
        assert "sid" in names
        assert "season" in names
        assert "lat" in names
        assert "lon" in names
        assert "iso_time" in names
        assert "usa_wind" in names
        assert "file_date" in names

    def test_double_fields(self):
        schema = pydantic_to_spark_schema(IbtacsCyclone)
        double_fields = {"lat", "lon", "usa_lat", "usa_lon"}
        for f in schema.fields:
            if f.name in double_fields:
                assert isinstance(f.dataType, DoubleType), f"{f.name} should be DoubleType"

    def test_int_fields(self):
        schema = pydantic_to_spark_schema(IbtacsCyclone)
        int_fields = {
            "season",
            "number",
            "wmo_wind",
            "wmo_pres",
            "dist2land",
            "landfall",
            "usa_wind",
            "usa_pres",
            "usa_sshs",
            "usa_r34_ne",
            "usa_gust",
            "storm_speed",
            "storm_dir",
        }
        for f in schema.fields:
            if f.name in int_fields:
                assert isinstance(f.dataType, LongType), f"{f.name} should be LongType"

    def test_timestamp_field(self):
        schema = pydantic_to_spark_schema(IbtacsCyclone)
        iso_time = next(f for f in schema.fields if f.name == "iso_time")
        assert isinstance(iso_time.dataType, TimestampType)


class TestIbtacsCycloneDDL:
    def test_ddl_types(self):
        ddl = pydantic_to_ddl(IbtacsCyclone, "ibtracs_cyclone", schema="geo")
        assert "sid TEXT NOT NULL" in ddl
        assert "season INTEGER NOT NULL" in ddl
        assert "lat DOUBLE PRECISION NOT NULL" in ddl
        assert "lon DOUBLE PRECISION NOT NULL" in ddl
        assert "iso_time TIMESTAMP NOT NULL" in ddl
        assert "usa_wind INTEGER NULL" in ddl
        assert "file_date TEXT NOT NULL" in ddl

    def test_ddl_creates_correct_table(self):
        ddl = pydantic_to_ddl(IbtacsCyclone, "ibtracs_cyclone", schema="geo")
        assert "CREATE TABLE IF NOT EXISTS geo.ibtracs_cyclone" in ddl


class TestIbtacsCycloneRegistry:
    def test_registered_in_factory(self):
        import model  # noqa: F401
        from model.plain.ibtracs_cyclone import IbtacsCyclone as IbtacsCycloneModel

        TableFactory.register("ibtracs_cyclone", IbtacsCycloneModel)
        assert "ibtracs_cyclone" in TableFactory.tables()
        assert TableFactory.get("ibtracs_cyclone") is IbtacsCycloneModel
