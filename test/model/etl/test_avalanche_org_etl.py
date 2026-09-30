import datetime as dt

import pytest
from pydantic import ValidationError
from pyspark.sql.types import DateType, DoubleType, StringType, TimestampType

from factory import TableFactory
from model.etl.avalanche_org_etl import AvalancheOrgEtl
from model.etl.event_etl import EventEtl
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema


class TestAvalancheOrgEtlValid:
    def test_full_row(self):
        row = AvalancheOrgEtl(
            source_name="AVALANCHE_ORG",
            subsource_name="Colorado Avalanche Information Center",
            event_name=None,
            event_category="avalanche",
            event_subcategory=None,
            event_description=None,
            latitude=39.55,
            longitude=-105.78,
            country_code="US",
            state_province="CO",
            place_location="Front Range",
            metric_name="magnitude",
            metric_value=None,
            metric_unit=None,
            danger_rating="considerable",
            statement="Dangerous avalanche conditions.",
            other_metrics=None,
            event_start_date=dt.date(2026, 1, 10),
            event_start=dt.datetime(2026, 1, 10, 6, 0, 0),
            event_end=dt.datetime(2026, 1, 11, 6, 0, 0),
            last_latitude=None,
            last_longitude=None,
            file_date="2026-01-10",
        )
        assert row.source_name == "AVALANCHE_ORG"
        assert row.country_code == "US"
        assert row.event_name is None
        assert row.place_location == "Front Range"
        assert row.event_start_date == dt.date(2026, 1, 10)
        assert row.file_date == "2026-01-10"

    def test_optional_fields_default_to_none(self):
        row = AvalancheOrgEtl(
            source_name="AVALANCHE_ORG",
            event_category="avalanche",
            country_code="US",
            metric_name="magnitude",
            file_date="2026-01-10",
        )
        assert row.event_name is None
        assert row.place_location is None
        assert row.metric_value is None
        assert row.danger_rating is None
        assert row.statement is None
        assert row.other_metrics is None
        assert row.state_province is None
        assert row.event_start_date is None
        assert row.event_start is None
        assert row.event_end is None


class TestAvalancheOrgEtlMissing:
    @pytest.mark.parametrize(
        "field",
        ["source_name", "event_category", "country_code", "metric_name", "file_date"],
    )
    def test_missing_required_field_raises(self, field):
        values = {
            "source_name": "AVALANCHE_ORG",
            "event_category": "avalanche",
            "country_code": "US",
            "metric_name": "magnitude",
            "file_date": "2026-01-10",
        }
        values.pop(field)
        with pytest.raises(ValidationError):
            AvalancheOrgEtl(**values)

    def test_empty_model_raises(self):
        with pytest.raises(ValidationError):
            AvalancheOrgEtl()


class TestAvalancheOrgEtlSchema:
    def test_inherits_the_canonical_structure(self):
        assert list(AvalancheOrgEtl.model_fields) == list(EventEtl.model_fields)

    def test_schema_field_count(self):
        schema = pydantic_to_spark_schema(AvalancheOrgEtl)
        assert len(schema.fields) == 23

    def test_schema_field_names(self):
        schema = pydantic_to_spark_schema(AvalancheOrgEtl)
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
        schema = pydantic_to_spark_schema(AvalancheOrgEtl)
        for name in ("source_name", "country_code", "place_location", "statement", "file_date"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, StringType), name

    def test_double_fields(self):
        schema = pydantic_to_spark_schema(AvalancheOrgEtl)
        for name in ("latitude", "longitude", "metric_value", "last_latitude", "last_longitude"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, DoubleType), name

    def test_date_field(self):
        schema = pydantic_to_spark_schema(AvalancheOrgEtl)
        field = next(f for f in schema.fields if f.name == "event_start_date")
        assert isinstance(field.dataType, DateType)

    def test_datetime_fields(self):
        schema = pydantic_to_spark_schema(AvalancheOrgEtl)
        for name in ("event_start", "event_end"):
            field = next(f for f in schema.fields if f.name == name)
            assert isinstance(field.dataType, TimestampType), name

    def test_required_fields_are_not_nullable(self):
        schema = pydantic_to_spark_schema(AvalancheOrgEtl)
        required = {"source_name", "event_category", "country_code", "metric_name", "file_date"}
        for f in schema.fields:
            assert f.nullable is (f.name not in required), f.name


class TestAvalancheOrgEtlDdl:
    def test_ddl_header(self):
        ddl = pydantic_to_ddl(AvalancheOrgEtl, "avalanche_org_etl", "geo")
        assert ddl.startswith("CREATE TABLE IF NOT EXISTS geo.avalanche_org_etl (")

    def test_ddl_column_types(self):
        ddl = pydantic_to_ddl(AvalancheOrgEtl, "avalanche_org_etl", "geo")
        assert "    country_code TEXT NOT NULL" in ddl
        assert "    place_location TEXT NULL" in ddl
        assert "    metric_value DOUBLE PRECISION NULL" in ddl
        assert "    event_start_date DATE NULL" in ddl
        assert "    event_start TIMESTAMP NULL" in ddl
        assert "    file_date TEXT NOT NULL" in ddl

    def test_registered_in_table_factory(self):
        TableFactory.register("avalanche_org_etl", AvalancheOrgEtl)
        assert "avalanche_org_etl" in TableFactory.tables()
        assert TableFactory.get("avalanche_org_etl") is AvalancheOrgEtl
