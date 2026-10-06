"""Family-shared tests for the nine canonical plain models.

DDL output, factory registration and missing-field validation are checked
per model; the row/schema/type contract lives in ``test_model_contract``.
"""

import datetime as dt

import pytest
from pydantic import ValidationError

from factory import TableFactory
from model.plain.avalanche_org import AvalancheOrg
from model.plain.eonet_event import EonetEvent
from model.plain.gdacs_event import GdacsEvent
from model.plain.ibtracs_cyclone import IbtacsCyclone
from model.plain.nifc_fire import NifcFire
from model.plain.openmeteo_flood import OpenMeteoFlood
from model.plain.usgs_earthquake import UsgsEarthquake
from model.plain.usgs_nwis import UsgsNwis
from model.plain.usgs_volcano import UsgsVolcano
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema

# (model, table name, required DDL fragments)
DDL_TYPE_CASES = [
    (
        AvalancheOrg,
        "avalanche_org",
        [
            "zone_id INTEGER NOT NULL",
            "zone_name TEXT NOT NULL",
            "danger_level INTEGER NOT NULL",
            "off_season BOOLEAN NOT NULL",
            "forecast_start TIMESTAMP NULL",
            "forecast_end TIMESTAMP NULL",
            "date DATE NOT NULL",
            "file_date TEXT NOT NULL",
            "warning TEXT NULL",
            "latitude DOUBLE PRECISION NULL",
            "longitude DOUBLE PRECISION NULL",
        ],
    ),
    (
        EonetEvent,
        "eonet_event",
        [
            "event_id TEXT NOT NULL",
            "latitude DOUBLE PRECISION NOT NULL",
            "longitude DOUBLE PRECISION NOT NULL",
            "event_date TIMESTAMP NOT NULL",
            "magnitude_value DOUBLE PRECISION NULL",
            "closed DATE NULL",
            "geometry_count INTEGER NOT NULL",
            "last_date TIMESTAMP NOT NULL",
            "file_date TEXT NOT NULL",
            "description TEXT NULL",
        ],
    ),
    (
        GdacsEvent,
        "gdacs_event",
        [
            "event_type TEXT NOT NULL",
            "event_id INTEGER NOT NULL",
            "latitude DOUBLE PRECISION NOT NULL",
            "longitude DOUBLE PRECISION NOT NULL",
            "from_date TIMESTAMP NOT NULL",
            "to_date TIMESTAMP NOT NULL",
            "date_modified TIMESTAMP NOT NULL",
            "severity DOUBLE PRECISION NOT NULL",
            "is_current BOOLEAN NOT NULL",
            "country TEXT NULL",
            "file_date TEXT NOT NULL",
        ],
    ),
    (
        IbtacsCyclone,
        "ibtracs_cyclone",
        [
            "sid TEXT NOT NULL",
            "season INTEGER NOT NULL",
            "lat DOUBLE PRECISION NOT NULL",
            "lon DOUBLE PRECISION NOT NULL",
            "iso_time TIMESTAMP NOT NULL",
            "usa_wind INTEGER NULL",
            "file_date TEXT NOT NULL",
        ],
    ),
    (
        NifcFire,
        "nifc_fire",
        [
            "acres DOUBLE PRECISION NULL",
            "fire_type TEXT NULL",
            "jurisdiction TEXT NULL",
            "unit_name TEXT NULL",
            "poly_date TIMESTAMP NULL",
            "file_date TEXT NOT NULL",
        ],
    ),
    (
        OpenMeteoFlood,
        "openmeteo_flood",
        [
            "latitude DOUBLE PRECISION NULL",
            "longitude DOUBLE PRECISION NULL",
            "discharge_m3s DOUBLE PRECISION NULL",
            "discharge_mean_m3s DOUBLE PRECISION NULL",
            "discharge_median_m3s DOUBLE PRECISION NULL",
            "discharge_p25_m3s DOUBLE PRECISION NULL",
            "discharge_p75_m3s DOUBLE PRECISION NULL",
            "date DATE NULL",
            "file_date TEXT NOT NULL",
        ],
    ),
    (
        UsgsEarthquake,
        "usgs_earthquake",
        [
            "latitude DOUBLE PRECISION NULL",
            "depth DOUBLE PRECISION NULL",
            "mag DOUBLE PRECISION NULL",
            "nst INTEGER NULL",
            "time TIMESTAMP NULL",
            "event_id TEXT NULL",
            "file_date TEXT NOT NULL",
        ],
    ),
    (
        UsgsNwis,
        "usgs_nwis",
        [
            "value DOUBLE PRECISION NULL",
            "latitude DOUBLE PRECISION NULL",
            "date DATE NULL",
            "file_date TEXT NOT NULL",
        ],
    ),
    (
        UsgsVolcano,
        "usgs_volcano",
        [
            "vnum INTEGER NULL",
            "notice_date TIMESTAMP NULL",
            "file_date TEXT NOT NULL",
        ],
    ),
]


# (model, table name registered in the factory)
TABLE_CASES = [
    (
        AvalancheOrg,
        "avalanche_org",
    ),
    (
        EonetEvent,
        "eonet_event",
    ),
    (
        GdacsEvent,
        "gdacs_event",
    ),
    (
        IbtacsCyclone,
        "ibtracs_cyclone",
    ),
    (
        NifcFire,
        "nifc_fire",
    ),
    (
        OpenMeteoFlood,
        "openmeteo_flood",
    ),
    (
        UsgsEarthquake,
        "usgs_earthquake",
    ),
    (
        UsgsNwis,
        "usgs_nwis",
    ),
    (
        UsgsVolcano,
        "usgs_volcano",
    ),
]


# (model, kwargs missing every required field)
MISSING_REQUIRED_CASES = [
    (
        AvalancheOrg,
        {"zone_name": "test"},
    ),
    (
        EonetEvent,
        {"event_name": "test"},
    ),
    (
        GdacsEvent,
        {"event_name": "test"},
    ),
    (
        IbtacsCyclone,
        {"name": "test"},
    ),
]


# (model, kwargs missing exactly one required field)
MISSING_KEY_CASES = [
    (
        AvalancheOrg,
        {
            "zone_name": "test",
            "center_id": "BAC",
            "center_name": "test",
            "state": "CA",
            "danger_level": -1,
            "danger_rating": "no rating",
            "travel_advice": "test",
            "off_season": False,
            "link": "http://test.com",
            "date": dt.date(2026, 1, 1),
            "file_date": "2026-01-01",
        },
    ),
    (
        EonetEvent,
        {
            "event_name": "test",
            "category_id": "test",
            "category_name": "Test",
            "source_id": "TEST",
            "latitude": 0.0,
            "longitude": 0.0,
            "event_date": dt.datetime(2026, 1, 1),
            "geometry_count": 0,
            "last_latitude": 0.0,
            "last_longitude": 0.0,
            "last_date": dt.datetime(2026, 1, 1),
            "file_date": "2026-01-01",
        },
    ),
    (
        GdacsEvent,
        {
            "event_type": "WF",
            "event_name": "test",
            "alert_level": "Green",
            "latitude": 0.0,
            "longitude": 0.0,
            "from_date": dt.datetime(2026, 1, 1),
            "to_date": dt.datetime(2026, 1, 1),
            "date_modified": dt.datetime(2026, 1, 1),
            "severity": 0.0,
            "source": "test",
            "report_url": "http://test.com",
            "is_current": True,
            "file_date": "2026-01-01",
        },
    ),
    (
        IbtacsCyclone,
        {
            "season": 2026,
            "number": 57,
            "basin": "EP",
            "iso_time": dt.datetime(2026, 9, 1),
            "lat": 17.7,
            "lon": -126.3,
            "file_date": "2026-09-01",
        },
    ),
]


class TestPlainMissing:
    @pytest.mark.parametrize("model_cls, kwargs", MISSING_REQUIRED_CASES)
    def test_missing_required_field_raises(self, model_cls, kwargs):
        # Assert
        with pytest.raises(ValidationError):
            model_cls(**kwargs)

    @pytest.mark.parametrize("model_cls, kwargs", MISSING_KEY_CASES)
    def test_missing_key_raises(self, model_cls, kwargs):
        # Assert
        with pytest.raises(ValidationError):
            model_cls(**kwargs)


class TestPlainDdl:
    @pytest.mark.parametrize("model_cls, table, fragments", DDL_TYPE_CASES)
    def test_ddl_types(self, model_cls, table, fragments):
        # Act
        ddl = pydantic_to_ddl(model_cls, table, schema="geo")
        # Assert
        for fragment in fragments:
            assert fragment in ddl

    @pytest.mark.parametrize("model_cls, table", TABLE_CASES)
    def test_ddl_creates_correct_table(self, model_cls, table):
        # Act
        ddl = pydantic_to_ddl(model_cls, table, schema="geo")
        # Assert
        assert f"CREATE TABLE IF NOT EXISTS geo.{table}" in ddl


class TestPlainRegistry:
    @pytest.mark.parametrize("model_cls, table", TABLE_CASES)
    def test_registered_in_factory(self, model_cls, table):
        # Act
        TableFactory.register(table, model_cls)
        # Assert
        assert table in TableFactory.tables()
        assert TableFactory.get(table) is model_cls


class TestPlainIbtracsSchema:
    def test_schema_field_names(self):
        # Act
        schema = pydantic_to_spark_schema(IbtacsCyclone)
        # Assert
        names = [f.name for f in schema.fields]
        for name in ("sid", "season", "lat", "lon", "iso_time", "usa_wind", "file_date"):
            assert name in names
