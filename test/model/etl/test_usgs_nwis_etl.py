import datetime as dt

import pytest
from pydantic import ValidationError
from pyspark.sql.types import DateType, DoubleType, StringType, TimestampType

from factory import TableFactory
from model.etl.event_etl import EventEtl
from model.etl.usgs_nwis_etl import UsgsNwisEtl
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema


class TestUsgsNwisEtlValid:
    def test_full_row(self):
        row = UsgsNwisEtl(
            source_name="USGS",
            subsource_name="USGS-01646500",
            event_name="Streamflow - Potomac river",
            event_category="flood",
            event_subcategory=None,
            event_description="provisional",
            latitude=39.0016667,
            longitude=-77.1591667,
            country_code="US",
            state_province="DC",
            place_location="Potomac river, WASH, DC LITTLE FALLS PUMP STA",
            metric_name="discharge",
            metric_value=1450.0,
            metric_unit="ft3/s",
            danger_rating=None,
            statement=None,
            other_metrics=None,
            event_start_date=dt.date(2026, 9, 1),
            event_start=dt.datetime(2026, 9, 1),
            event_end=None,
            last_latitude=None,
            last_longitude=None,
            file_date="2026-09-01",
        )
        assert row.source_name == "USGS"
        assert row.subsource_name == "USGS-01646500"
        assert row.event_name == "Streamflow - Potomac river"
        assert row.event_category == "flood"
        assert row.event_description == "provisional"
        assert row.country_code == "US"
        assert row.state_province == "DC"
        assert row.place_location == "Potomac river, WASH, DC LITTLE FALLS PUMP STA"
        assert row.metric_name == "discharge"
        assert row.metric_value == 1450.0
        assert row.metric_unit == "ft3/s"
        assert row.other_metrics is None
        assert row.event_end is None

    def test_optional_fields_default_to_none(self):
        row = UsgsNwisEtl(
            source_name="USGS",
            event_category="flood",
            country_code="US",
            metric_name="discharge",
            file_date="2026-09-01",
        )
        assert row.subsource_name is None
        assert row.event_name is None
        assert row.event_subcategory is None
        assert row.event_description is None
        assert row.latitude is None
        assert row.longitude is None
        assert row.state_province is None
        assert row.place_location is None
        assert row.metric_value is None
        assert row.metric_unit is None
        assert row.danger_rating is None
        assert row.statement is None
        assert row.other_metrics is None
        assert row.event_start_date is None
        assert row.event_start is None
        assert row.event_end is None
        assert row.last_latitude is None
        assert row.last_longitude is None


class TestUsgsNwisEtlMissing:
    @pytest.mark.parametrize(
        "field",
        ["source_name", "event_category", "country_code", "metric_name", "file_date"],
    )
    def test_missing_required_field_raises(self, field):
        values = {
            "source_name": "USGS",
            "event_category": "flood",
            "country_code": "US",
            "metric_name": "discharge",
            "file_date": "2026-09-01",
        }
        values.pop(field)
        with pytest.raises(ValidationError):
            UsgsNwisEtl(**values)

    def test_empty_model_raises(self):
        with pytest.raises(ValidationError):
            UsgsNwisEtl()


class TestUsgsNwisEtlSchema:
    def test_inherits_the_canonical_structure(self):
        assert list(UsgsNwisEtl.model_fields) == list(EventEtl.model_fields)

    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(UsgsNwisEtl)
        assert len(schema.fields) == 23

    def test_schema_field_names(self):
        schema = pydantic_to_spark_schema(UsgsNwisEtl)
        names = [f.name for f in schema.fields]
        assert names == [
            "source_name",
            "subsource_name",
            "event_name",
            "event_category",
            "event_subcategory",
            "event_description",
            "latitude",
            "longitude",
            "country_code",
            "state_province",
            "place_location",
            "metric_name",
            "metric_value",
            "metric_unit",
            "danger_rating",
            "statement",
            "other_metrics",
            "event_start_date",
            "event_start",
            "event_end",
            "last_latitude",
            "last_longitude",
            "file_date",
        ]

    def test_string_fields(self):
        schema = pydantic_to_spark_schema(UsgsNwisEtl)
        for name in ("source_name", "country_code", "place_location", "other_metrics", "file_date"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, StringType), name

    def test_double_fields(self):
        schema = pydantic_to_spark_schema(UsgsNwisEtl)
        for name in ("latitude", "longitude", "metric_value", "last_latitude", "last_longitude"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, DoubleType), name

    def test_date_field(self):
        schema = pydantic_to_spark_schema(UsgsNwisEtl)
        field = next(f for f in schema.fields if f.name == "event_start_date")
        assert isinstance(field.dataType, DateType)

    def test_datetime_fields(self):
        schema = pydantic_to_spark_schema(UsgsNwisEtl)
        for name in ("event_start", "event_end"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, TimestampType), name

    def test_required_fields_are_not_nullable(self):
        schema = pydantic_to_spark_schema(UsgsNwisEtl)
        required = {"source_name", "event_category", "country_code", "metric_name", "file_date"}
        for f in schema.fields:
            assert f.nullable is (f.name not in required), f.name


class TestUsgsNwisEtlDdl:
    def test_ddl_header(self):
        ddl = pydantic_to_ddl(UsgsNwisEtl, "usgs_nwis_etl", "geo")
        assert ddl.startswith("CREATE TABLE IF NOT EXISTS geo.usgs_nwis_etl (")

    def test_ddl_column_types(self):
        ddl = pydantic_to_ddl(UsgsNwisEtl, "usgs_nwis_etl", "geo")
        assert "    country_code TEXT NOT NULL" in ddl
        assert "    place_location TEXT NULL" in ddl
        assert "    metric_value DOUBLE PRECISION NULL" in ddl
        assert "    event_start_date DATE NULL" in ddl
        assert "    event_start TIMESTAMP NULL" in ddl
        assert "    file_date TEXT NOT NULL" in ddl

    def test_registered_in_table_factory(self):
        TableFactory.register("usgs_nwis_etl", UsgsNwisEtl)
        assert "usgs_nwis_etl" in TableFactory.tables()
        assert TableFactory.get("usgs_nwis_etl") is UsgsNwisEtl
