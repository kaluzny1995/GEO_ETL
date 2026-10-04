import pytest
from pydantic import ValidationError
from pyspark.sql.types import StringType

from model.raw.ibtracs_cyclone_raw import IbtacsCycloneRaw
from utils.schema_converter import pydantic_to_spark_schema

VALID_ROW = {
    "SID": "2026237N09256",
    "SEASON": "2026",
    "NUMBER": "57",
    "BASIN": "EP",
    "SUBBASIN": "MM",
    "NAME": "KARINA",
    "ISO_TIME": "2026-09-01 00:00:00",
    "NATURE": "TS",
    "LAT": "17.7",
    "LON": "-126.3",
    "WMO_WIND": " ",
    "WMO_PRES": " ",
    "WMO_AGENCY": " ",
    "TRACK_TYPE": "PROVISIONAL",
    "DIST2LAND": "1608",
    "LANDFALL": "1608",
    "IFLAG": "O______________",
    "USA_AGENCY": "nhc_working_bt",
    "USA_ATCF_ID": "EP112026",
    "USA_LAT": "17.7",
    "USA_LON": "-126.3",
    "USA_RECORD": " ",
    "USA_STATUS": "HU",
    "USA_WIND": "115",
    "USA_PRES": "950",
    "USA_SSHS": "4",
    "USA_R34_NE": "100",
    "USA_R34_SE": "80",
    "USA_R34_SW": "70",
    "USA_R34_NW": "100",
    "USA_R50_NE": "50",
    "USA_R50_SE": "35",
    "USA_R50_SW": "30",
    "USA_R50_NW": "50",
    "USA_R64_NE": "25",
    "USA_R64_SE": "15",
    "USA_R64_SW": "15",
    "USA_R64_NW": "25",
    "USA_POCI": "1007",
    "USA_ROCI": "250",
    "USA_RMW": "15",
    "USA_EYE": " ",
    "TOKYO_LAT": " ",
    "TOKYO_LON": " ",
    "TOKYO_GRADE": " ",
    "TOKYO_WIND": " ",
    "TOKYO_PRES": " ",
    "TOKYO_R50_DIR": " ",
    "TOKYO_R50_LONG": " ",
    "TOKYO_R50_SHORT": " ",
    "TOKYO_R30_DIR": " ",
    "TOKYO_R30_LONG": " ",
    "TOKYO_R30_SHORT": " ",
    "TOKYO_LAND": " ",
    "CMA_LAT": " ",
    "CMA_LON": " ",
    "CMA_CAT": " ",
    "CMA_WIND": " ",
    "CMA_PRES": " ",
    "HKO_LAT": " ",
    "HKO_LON": " ",
    "HKO_CAT": " ",
    "HKO_WIND": " ",
    "HKO_PRES": " ",
    "KMA_LAT": " ",
    "KMA_LON": " ",
    "KMA_CAT": " ",
    "KMA_WIND": " ",
    "KMA_PRES": " ",
    "KMA_R50_DIR": " ",
    "KMA_R50_LONG": " ",
    "KMA_R50_SHORT": " ",
    "KMA_R30_DIR": " ",
    "KMA_R30_LONG": " ",
    "KMA_R30_SHORT": " ",
    "NEWDELHI_LAT": " ",
    "NEWDELHI_LON": " ",
    "NEWDELHI_GRADE": " ",
    "NEWDELHI_WIND": " ",
    "NEWDELHI_PRES": " ",
    "NEWDELHI_CI": " ",
    "NEWDELHI_DP": " ",
    "NEWDELHI_POCI": " ",
    "REUNION_LAT": " ",
    "REUNION_LON": " ",
    "REUNION_TYPE": " ",
    "REUNION_WIND": " ",
    "REUNION_PRES": " ",
    "REUNION_TNUM": " ",
    "REUNION_CI": " ",
    "REUNION_RMW": " ",
    "REUNION_R34_NE": " ",
    "REUNION_R34_SE": " ",
    "REUNION_R34_SW": " ",
    "REUNION_R34_NW": " ",
    "REUNION_R50_NE": " ",
    "REUNION_R50_SE": " ",
    "REUNION_R50_SW": " ",
    "REUNION_R50_NW": " ",
    "REUNION_R64_NE": " ",
    "REUNION_R64_SE": " ",
    "REUNION_R64_SW": " ",
    "REUNION_R64_NW": " ",
    "BOM_LAT": " ",
    "BOM_LON": " ",
    "BOM_TYPE": " ",
    "BOM_WIND": " ",
    "BOM_PRES": " ",
    "BOM_TNUM": " ",
    "BOM_CI": " ",
    "BOM_RMW": " ",
    "BOM_R34_NE": " ",
    "BOM_R34_SE": " ",
    "BOM_R34_SW": " ",
    "BOM_R34_NW": " ",
    "BOM_R50_NE": " ",
    "BOM_R50_SE": " ",
    "BOM_R50_SW": " ",
    "BOM_R50_NW": " ",
    "BOM_R64_NE": " ",
    "BOM_R64_SE": " ",
    "BOM_R64_SW": " ",
    "BOM_R64_NW": " ",
    "BOM_ROCI": " ",
    "BOM_POCI": " ",
    "BOM_EYE": " ",
    "BOM_POS_METHOD": " ",
    "BOM_PRES_METHOD": " ",
    "NADI_LAT": " ",
    "NADI_LON": " ",
    "NADI_CAT": " ",
    "NADI_WIND": " ",
    "NADI_PRES": " ",
    "WELLINGTON_LAT": " ",
    "WELLINGTON_LON": " ",
    "WELLINGTON_WIND": " ",
    "WELLINGTON_PRES": " ",
    "DS824_LAT": " ",
    "DS824_LON": " ",
    "DS824_STAGE": " ",
    "DS824_WIND": " ",
    "DS824_PRES": " ",
    "TD9636_LAT": " ",
    "TD9636_LON": " ",
    "TD9636_STAGE": " ",
    "TD9636_WIND": " ",
    "TD9636_PRES": " ",
    "TD9635_LAT": " ",
    "TD9635_LON": " ",
    "TD9635_WIND": " ",
    "TD9635_PRES": " ",
    "TD9635_ROCI": " ",
    "NEUMANN_LAT": " ",
    "NEUMANN_LON": " ",
    "NEUMANN_CLASS": " ",
    "NEUMANN_WIND": " ",
    "NEUMANN_PRES": " ",
    "MLC_LAT": " ",
    "MLC_LON": " ",
    "MLC_CLASS": " ",
    "MLC_WIND": " ",
    "MLC_PRES": " ",
    "USA_GUST": "140",
    "BOM_GUST": " ",
    "BOM_GUST_PER": " ",
    "REUNION_GUST": " ",
    "REUNION_GUST_PER": " ",
    "USA_SEAHGT": " ",
    "USA_SEARAD_NE": "12",
    "USA_SEARAD_SE": "300",
    "USA_SEARAD_SW": "180",
    "USA_SEARAD_NW": "210",
    "STORM_SPEED": "270",
    "STORM_DIR": "10",
    "file_date": "2026-09-01",
}


class TestIbtacsCycloneRawValid:
    def test_valid_row(self):
        model = IbtacsCycloneRaw(**VALID_ROW)
        assert model.SID == "2026237N09256"
        assert model.NAME == "KARINA"
        assert model.USA_WIND == "115"
        assert model.file_date == "2026-09-01"

    def test_optional_fields_none(self):
        row = {k: v for k, v in VALID_ROW.items() if k not in ("TOKYO_LAT", "BOM_LAT", "USA_GUST")}
        row["TOKYO_LAT"] = None
        row["BOM_LAT"] = None
        row["USA_GUST"] = None
        model = IbtacsCycloneRaw(**row)
        assert model.TOKYO_LAT is None
        assert model.BOM_LAT is None

    def test_all_174_fields_accepted(self):
        model = IbtacsCycloneRaw(**VALID_ROW)
        field_names = [k for k in VALID_ROW if k != "file_date"]
        for name in field_names:
            assert getattr(model, name) == VALID_ROW[name]


class TestIbtacsCycloneRawMissing:
    def test_missing_required_field_raises(self):
        row = {k: v for k, v in VALID_ROW.items() if k != "SID"}
        with pytest.raises(ValidationError):
            IbtacsCycloneRaw(**row)

    def test_missing_file_date_raises(self):
        row = {k: v for k, v in VALID_ROW.items() if k != "file_date"}
        with pytest.raises(ValidationError):
            IbtacsCycloneRaw(**row)


class TestIbtacsCycloneRawSparkSchema:
    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(IbtacsCycloneRaw)
        assert len(schema.fields) == 177

    def test_all_fields_are_string_type(self):
        schema = pydantic_to_spark_schema(IbtacsCycloneRaw)
        for field in schema.fields:
            assert isinstance(field.dataType, StringType), f"{field.name} is not StringType"
