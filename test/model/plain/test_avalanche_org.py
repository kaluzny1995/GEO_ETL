import datetime as dt

import pytest
from pydantic import ValidationError
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    TimestampType,
)

from factory import TableFactory
from model.plain.avalanche_org import AvalancheOrg
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema


class TestAvalancheOrgValid:
    def test_valid_row(self):
        row = AvalancheOrg(
            zone_id=3004,
            zone_name="Bridgeport Avalanche Center",
            center_id="BAC",
            center_name="Bridgeport Avalanche Center",
            state="CA",
            danger_level=-1,
            danger_rating="no rating",
            travel_advice="Watch for signs of unstable snow.",
            off_season=False,
            forecast_start=dt.datetime(2026, 4, 13, 14, 0, 0),
            forecast_end=dt.datetime(2026, 11, 1, 14, 30, 0),
            warning=None,
            link="https://bridgeportavalanchecenter.org/avalanche-forecast/",
            latitude=38.255931,
            longitude=-119.451337,
            date=dt.date(2026, 9, 20),
            file_date="2026-09-20",
        )
        assert row.zone_id == 3004
        assert row.off_season is False
        assert row.date == dt.date(2026, 9, 20)
        assert row.latitude == 38.255931
        assert row.longitude == -119.451337

    def test_optional_fields_none(self):
        row = AvalancheOrg(
            zone_id=2856,
            zone_name="Salt River and Wyoming Ranges",
            center_id="BTAC",
            center_name="Bridger-Teton Avalanche Center",
            state="WY",
            danger_level=-1,
            danger_rating="no rating",
            travel_advice="Watch for signs.",
            off_season=True,
            link="https://bridgertetonavalanchecenter.org/",
            date=dt.date(2026, 9, 20),
            file_date="2026-09-20",
        )
        assert row.forecast_start is None
        assert row.forecast_end is None
        assert row.warning is None
        assert row.latitude is None
        assert row.longitude is None


class TestAvalancheOrgMissing:
    def test_missing_required_field_raises(self):
        with pytest.raises(ValidationError):
            AvalancheOrg(zone_name="test")

    def test_missing_zone_id_raises(self):
        with pytest.raises(ValidationError):
            AvalancheOrg(
                zone_name="test",
                center_id="BAC",
                center_name="test",
                state="CA",
                danger_level=-1,
                danger_rating="no rating",
                travel_advice="test",
                off_season=False,
                link="http://test.com",
                date=dt.date(2026, 1, 1),
                file_date="2026-01-01",
            )


class TestAvalancheOrgSparkSchema:
    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(AvalancheOrg)
        assert len(schema.fields) == 17

    def test_schema_field_names(self):
        schema = pydantic_to_spark_schema(AvalancheOrg)
        names = [f.name for f in schema.fields]
        assert names == [
            "zone_id",
            "zone_name",
            "center_id",
            "center_name",
            "state",
            "danger_level",
            "danger_rating",
            "travel_advice",
            "off_season",
            "forecast_start",
            "forecast_end",
            "warning",
            "link",
            "latitude",
            "longitude",
            "date",
            "file_date",
        ]

    def test_int_fields_are_long_type(self):
        schema = pydantic_to_spark_schema(AvalancheOrg)
        zone_id_field = next(f for f in schema.fields if f.name == "zone_id")
        danger_level_field = next(f for f in schema.fields if f.name == "danger_level")
        assert isinstance(zone_id_field.dataType, LongType)
        assert isinstance(danger_level_field.dataType, LongType)

    def test_coordinate_fields_are_double_type(self):
        schema = pydantic_to_spark_schema(AvalancheOrg)
        latitude_field = next(f for f in schema.fields if f.name == "latitude")
        longitude_field = next(f for f in schema.fields if f.name == "longitude")
        assert isinstance(latitude_field.dataType, DoubleType)
        assert isinstance(longitude_field.dataType, DoubleType)

    def test_bool_field(self):
        schema = pydantic_to_spark_schema(AvalancheOrg)
        off_season_field = next(f for f in schema.fields if f.name == "off_season")
        assert isinstance(off_season_field.dataType, BooleanType)

    def test_datetime_fields(self):
        schema = pydantic_to_spark_schema(AvalancheOrg)
        forecast_start = next(f for f in schema.fields if f.name == "forecast_start")
        forecast_end = next(f for f in schema.fields if f.name == "forecast_end")
        assert isinstance(forecast_start.dataType, TimestampType)
        assert isinstance(forecast_end.dataType, TimestampType)

    def test_date_field(self):
        schema = pydantic_to_spark_schema(AvalancheOrg)
        date_field = next(f for f in schema.fields if f.name == "date")
        assert isinstance(date_field.dataType, DateType)

    def test_nullable_fields(self):
        schema = pydantic_to_spark_schema(AvalancheOrg)
        nullable_names = {"forecast_start", "forecast_end", "warning", "latitude", "longitude"}
        for f in schema.fields:
            if f.name in nullable_names:
                assert f.nullable is True, f"{f.name} should be nullable"
            else:
                assert f.nullable is False, f"{f.name} should not be nullable"


class TestAvalancheOrgDDL:
    def test_ddl_types(self):
        ddl = pydantic_to_ddl(AvalancheOrg, "avalanche_org", schema="geo")
        assert "zone_id INTEGER NOT NULL" in ddl
        assert "zone_name TEXT NOT NULL" in ddl
        assert "danger_level INTEGER NOT NULL" in ddl
        assert "off_season BOOLEAN NOT NULL" in ddl
        assert "forecast_start TIMESTAMP NULL" in ddl
        assert "forecast_end TIMESTAMP NULL" in ddl
        assert "date DATE NOT NULL" in ddl
        assert "file_date TEXT NOT NULL" in ddl
        assert "warning TEXT NULL" in ddl
        assert "latitude DOUBLE PRECISION NULL" in ddl
        assert "longitude DOUBLE PRECISION NULL" in ddl

    def test_ddl_creates_correct_table(self):
        ddl = pydantic_to_ddl(AvalancheOrg, "avalanche_org", schema="geo")
        assert "CREATE TABLE IF NOT EXISTS geo.avalanche_org" in ddl


class TestAvalancheOrgRegistry:
    def test_registered_in_factory(self):
        TableFactory.register("avalanche_org", AvalancheOrg)
        assert "avalanche_org" in TableFactory.tables()
        assert TableFactory.get("avalanche_org") is AvalancheOrg
