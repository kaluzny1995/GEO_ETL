import pytest
from pydantic import ValidationError

from factory import TableFactory
from model.etl.avalanche_org_etl import AvalancheOrgEtl
from model.etl.eonet_event_etl import EonetEventEtl
from model.etl.event_etl import EventEtl
from model.etl.gdacs_event_etl import GdacsEventEtl
from model.etl.ibtracs_cyclone_etl import IbtracsCycloneEtl
from model.etl.nifc_fire_etl import NifcFireEtl
from model.etl.openmeteo_flood_etl import OpenMeteoFloodEtl
from model.etl.usgs_earthquake_etl import UsgsEarthquakeEtl
from model.etl.usgs_nwis_etl import UsgsNwisEtl
from model.etl.usgs_volcano_etl import UsgsVolcanoEtl
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema

MODEL_CLASSES = [
    AvalancheOrgEtl,
    EonetEventEtl,
    GdacsEventEtl,
    IbtracsCycloneEtl,
    NifcFireEtl,
    OpenMeteoFloodEtl,
    UsgsEarthquakeEtl,
    UsgsNwisEtl,
    UsgsVolcanoEtl,
]

EXPECTED_FIELD_NAMES = [
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
    "other_metrics",
    "event_start",
    "event_end",
    "last_latitude",
    "last_longitude",
    "file_date",
]

REQUIRED_FIELDS = {"country_code", "event_category", "file_date", "metric_name", "source_name"}

MISSING_CASES = [
    (
        AvalancheOrgEtl,
        {
            "source_name": "AVALANCHE_ORG",
            "event_category": "avalanche",
            "country_code": "US",
            "metric_name": "magnitude",
            "file_date": "2026-01-10",
        },
        "source_name",
    ),
    (
        AvalancheOrgEtl,
        {
            "source_name": "AVALANCHE_ORG",
            "event_category": "avalanche",
            "country_code": "US",
            "metric_name": "magnitude",
            "file_date": "2026-01-10",
        },
        "event_category",
    ),
    (
        AvalancheOrgEtl,
        {
            "source_name": "AVALANCHE_ORG",
            "event_category": "avalanche",
            "country_code": "US",
            "metric_name": "magnitude",
            "file_date": "2026-01-10",
        },
        "country_code",
    ),
    (
        AvalancheOrgEtl,
        {
            "source_name": "AVALANCHE_ORG",
            "event_category": "avalanche",
            "country_code": "US",
            "metric_name": "magnitude",
            "file_date": "2026-01-10",
        },
        "metric_name",
    ),
    (
        EonetEventEtl,
        {
            "source_name": "EONET",
            "event_category": "storm",
            "country_code": "ZZ",
            "metric_name": "wind speed",
            "file_date": "2026-09-21",
        },
        "source_name",
    ),
    (
        EonetEventEtl,
        {
            "source_name": "EONET",
            "event_category": "storm",
            "country_code": "ZZ",
            "metric_name": "wind speed",
            "file_date": "2026-09-21",
        },
        "event_category",
    ),
    (
        EonetEventEtl,
        {
            "source_name": "EONET",
            "event_category": "storm",
            "country_code": "ZZ",
            "metric_name": "wind speed",
            "file_date": "2026-09-21",
        },
        "country_code",
    ),
    (
        EonetEventEtl,
        {
            "source_name": "EONET",
            "event_category": "storm",
            "country_code": "ZZ",
            "metric_name": "wind speed",
            "file_date": "2026-09-21",
        },
        "metric_name",
    ),
    (
        GdacsEventEtl,
        {
            "source_name": "GDACS",
            "event_category": "earthquake",
            "country_code": "ZZ",
            "metric_name": "magnitude",
            "file_date": "2026-09-29",
        },
        "source_name",
    ),
    (
        GdacsEventEtl,
        {
            "source_name": "GDACS",
            "event_category": "earthquake",
            "country_code": "ZZ",
            "metric_name": "magnitude",
            "file_date": "2026-09-29",
        },
        "event_category",
    ),
    (
        GdacsEventEtl,
        {
            "source_name": "GDACS",
            "event_category": "earthquake",
            "country_code": "ZZ",
            "metric_name": "magnitude",
            "file_date": "2026-09-29",
        },
        "country_code",
    ),
    (
        GdacsEventEtl,
        {
            "source_name": "GDACS",
            "event_category": "earthquake",
            "country_code": "ZZ",
            "metric_name": "magnitude",
            "file_date": "2026-09-29",
        },
        "metric_name",
    ),
    (
        IbtracsCycloneEtl,
        {
            "source_name": "IBTRACS",
            "event_category": "storm",
            "country_code": "ZZ",
            "metric_name": "wind speed",
            "file_date": "2026-09-01",
        },
        "source_name",
    ),
    (
        IbtracsCycloneEtl,
        {
            "source_name": "IBTRACS",
            "event_category": "storm",
            "country_code": "ZZ",
            "metric_name": "wind speed",
            "file_date": "2026-09-01",
        },
        "event_category",
    ),
    (
        IbtracsCycloneEtl,
        {
            "source_name": "IBTRACS",
            "event_category": "storm",
            "country_code": "ZZ",
            "metric_name": "wind speed",
            "file_date": "2026-09-01",
        },
        "country_code",
    ),
    (
        IbtracsCycloneEtl,
        {
            "source_name": "IBTRACS",
            "event_category": "storm",
            "country_code": "ZZ",
            "metric_name": "wind speed",
            "file_date": "2026-09-01",
        },
        "metric_name",
    ),
    (
        NifcFireEtl,
        {
            "source_name": "NIFC",
            "event_category": "fire",
            "country_code": "US",
            "metric_name": "area",
            "file_date": "2026-09-01",
        },
        "source_name",
    ),
    (
        NifcFireEtl,
        {
            "source_name": "NIFC",
            "event_category": "fire",
            "country_code": "US",
            "metric_name": "area",
            "file_date": "2026-09-01",
        },
        "event_category",
    ),
    (
        NifcFireEtl,
        {
            "source_name": "NIFC",
            "event_category": "fire",
            "country_code": "US",
            "metric_name": "area",
            "file_date": "2026-09-01",
        },
        "country_code",
    ),
    (
        NifcFireEtl,
        {
            "source_name": "NIFC",
            "event_category": "fire",
            "country_code": "US",
            "metric_name": "area",
            "file_date": "2026-09-01",
        },
        "metric_name",
    ),
    (
        OpenMeteoFloodEtl,
        {
            "source_name": "OPENMETEO",
            "event_category": "flood",
            "country_code": "BR",
            "metric_name": "discharge",
            "file_date": "2026-09-01",
        },
        "source_name",
    ),
    (
        OpenMeteoFloodEtl,
        {
            "source_name": "OPENMETEO",
            "event_category": "flood",
            "country_code": "BR",
            "metric_name": "discharge",
            "file_date": "2026-09-01",
        },
        "event_category",
    ),
    (
        OpenMeteoFloodEtl,
        {
            "source_name": "OPENMETEO",
            "event_category": "flood",
            "country_code": "BR",
            "metric_name": "discharge",
            "file_date": "2026-09-01",
        },
        "country_code",
    ),
    (
        OpenMeteoFloodEtl,
        {
            "source_name": "OPENMETEO",
            "event_category": "flood",
            "country_code": "BR",
            "metric_name": "discharge",
            "file_date": "2026-09-01",
        },
        "metric_name",
    ),
    (
        UsgsEarthquakeEtl,
        {
            "source_name": "USGS",
            "event_category": "earthquake",
            "country_code": "US",
            "metric_name": "magnitude",
            "file_date": "2026-09-01",
        },
        "source_name",
    ),
    (
        UsgsEarthquakeEtl,
        {
            "source_name": "USGS",
            "event_category": "earthquake",
            "country_code": "US",
            "metric_name": "magnitude",
            "file_date": "2026-09-01",
        },
        "event_category",
    ),
    (
        UsgsEarthquakeEtl,
        {
            "source_name": "USGS",
            "event_category": "earthquake",
            "country_code": "US",
            "metric_name": "magnitude",
            "file_date": "2026-09-01",
        },
        "country_code",
    ),
    (
        UsgsEarthquakeEtl,
        {
            "source_name": "USGS",
            "event_category": "earthquake",
            "country_code": "US",
            "metric_name": "magnitude",
            "file_date": "2026-09-01",
        },
        "metric_name",
    ),
    (
        UsgsNwisEtl,
        {
            "source_name": "USGS",
            "event_category": "flood",
            "country_code": "US",
            "metric_name": "discharge",
            "file_date": "2026-09-01",
        },
        "source_name",
    ),
    (
        UsgsNwisEtl,
        {
            "source_name": "USGS",
            "event_category": "flood",
            "country_code": "US",
            "metric_name": "discharge",
            "file_date": "2026-09-01",
        },
        "event_category",
    ),
    (
        UsgsNwisEtl,
        {
            "source_name": "USGS",
            "event_category": "flood",
            "country_code": "US",
            "metric_name": "discharge",
            "file_date": "2026-09-01",
        },
        "country_code",
    ),
    (
        UsgsNwisEtl,
        {
            "source_name": "USGS",
            "event_category": "flood",
            "country_code": "US",
            "metric_name": "discharge",
            "file_date": "2026-09-01",
        },
        "metric_name",
    ),
    (
        UsgsVolcanoEtl,
        {
            "source_name": "USGS",
            "event_category": "volcano",
            "country_code": "US",
            "metric_name": "vei",
            "file_date": "2026-09-29",
        },
        "source_name",
    ),
    (
        UsgsVolcanoEtl,
        {
            "source_name": "USGS",
            "event_category": "volcano",
            "country_code": "US",
            "metric_name": "vei",
            "file_date": "2026-09-29",
        },
        "event_category",
    ),
    (
        UsgsVolcanoEtl,
        {
            "source_name": "USGS",
            "event_category": "volcano",
            "country_code": "US",
            "metric_name": "vei",
            "file_date": "2026-09-29",
        },
        "country_code",
    ),
    (
        UsgsVolcanoEtl,
        {
            "source_name": "USGS",
            "event_category": "volcano",
            "country_code": "US",
            "metric_name": "vei",
            "file_date": "2026-09-29",
        },
        "metric_name",
    ),
]

DDL_CASES = [
    (AvalancheOrgEtl, "avalanche_org_etl"),
    (EonetEventEtl, "eonet_event_etl"),
    (GdacsEventEtl, "gdacs_event_etl"),
    (IbtracsCycloneEtl, "ibtracs_cyclone_etl"),
    (NifcFireEtl, "nifc_fire_etl"),
    (OpenMeteoFloodEtl, "openmeteo_flood_etl"),
    (UsgsEarthquakeEtl, "usgs_earthquake_etl"),
    (UsgsNwisEtl, "usgs_nwis_etl"),
    (UsgsVolcanoEtl, "usgs_volcano_etl"),
]


class TestEventEtlMissing:
    @pytest.mark.parametrize("model_cls, values, field", MISSING_CASES)
    def test_missing_required_field_raises(self, model_cls, values, field):
        # Act
        payload = {key: value for key, value in values.items() if key != field}
        # Assert
        with pytest.raises(ValidationError):
            model_cls(**payload)

    @pytest.mark.parametrize("model_cls", MODEL_CLASSES)
    def test_empty_model_raises(self, model_cls):
        # Assert
        with pytest.raises(ValidationError):
            model_cls()


class TestEventEtlSchema:
    @pytest.mark.parametrize("model_cls", MODEL_CLASSES)
    def test_inherits_the_canonical_structure(self, model_cls):
        # Assert
        assert list(model_cls.model_fields) == list(EventEtl.model_fields)

    @pytest.mark.parametrize("model_cls", MODEL_CLASSES)
    def test_schema_field_names(self, model_cls):
        # Act
        schema = pydantic_to_spark_schema(model_cls)
        # Assert
        assert [field.name for field in schema.fields] == EXPECTED_FIELD_NAMES

    @pytest.mark.parametrize("model_cls", MODEL_CLASSES)
    def test_required_fields_are_not_nullable(self, model_cls):
        # Act
        schema = pydantic_to_spark_schema(model_cls)
        # Assert
        for field in schema.fields:
            assert field.nullable is (field.name not in REQUIRED_FIELDS), field.name


class TestEventEtlDdl:
    @pytest.mark.parametrize("model_cls, table", DDL_CASES)
    def test_ddl_header(self, model_cls, table):
        # Act
        ddl = pydantic_to_ddl(model_cls, table, "geo")
        # Assert
        assert ddl.startswith(f"CREATE TABLE IF NOT EXISTS geo.{table} (")

    @pytest.mark.parametrize("model_cls, table", DDL_CASES)
    def test_ddl_column_types(self, model_cls, table):
        # Act
        ddl = pydantic_to_ddl(model_cls, table, "geo")
        # Assert
        assert "    country_code TEXT NOT NULL" in ddl
        assert "    place_location TEXT NULL" in ddl
        assert "    metric_value DOUBLE PRECISION NULL" in ddl
        assert "    event_start TIMESTAMP NULL" in ddl
        assert "    file_date TEXT NOT NULL" in ddl

    @pytest.mark.parametrize("model_cls, table", DDL_CASES)
    def test_registered_in_table_factory(self, model_cls, table):
        # Act
        TableFactory.register(table, model_cls)
        # Assert
        assert table in TableFactory.tables()
        assert TableFactory.get(table) is model_cls
