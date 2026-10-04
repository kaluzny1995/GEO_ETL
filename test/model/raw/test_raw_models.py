"""Family-shared tests for the nine canonical raw models.

Every field is a string, optional rows pass through untouched and the
NIFC CSV column order matches the download header; the row/schema
contract lives in ``test_model_contract``.
"""

import pytest
from pydantic import ValidationError
from pyspark.sql.types import StringType

from model.raw.avalanche_org_raw import AvalancheOrgRaw
from model.raw.eonet_event_raw import EonetEventRaw
from model.raw.gdacs_event_raw import GdacsEventRaw
from model.raw.ibtracs_cyclone_raw import IbtacsCycloneRaw
from model.raw.nifc_fire_raw import NifcFireRaw
from model.raw.openmeteo_flood_raw import OpenMeteoFloodRaw
from model.raw.usgs_earthquake_raw import UsgsEarthquakeRaw
from model.raw.usgs_nwis_raw import UsgsNwisRaw
from model.raw.usgs_volcano_raw import UsgsVolcanoRaw
from test.model.test_model_contract import VALID_ROWS
from utils.schema_converter import pydantic_to_spark_schema

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
    "geocode_state",
]


# (model, required field removed from the valid row)
MISSING_KEY_CASES = [
    (AvalancheOrgRaw, "zone_id"),
    (EonetEventRaw, "event_id"),
    (GdacsEventRaw, "event_id"),
    (IbtacsCycloneRaw, "SID"),
]


# (model, optional fields that must carry their row value)
OPTIONAL_PRESENT_CASES = [
    (
        AvalancheOrgRaw,
        {
            "forecast_start": "2026-04-13T14:00:00",
            "forecast_end": "2026-11-01T14:30:00",
        },
    ),
    (
        EonetEventRaw,
        {
            "magnitude_value": "1100.0",
            "magnitude_unit": "acres",
        },
    ),
    (
        GdacsEventRaw,
        {
            "country": "Australia",
            "severity_unit": "ha",
        },
    ),
]


# (models that accept their entire valid row)
ALL_ACCEPTED_CASES = [
    IbtacsCycloneRaw,
    NifcFireRaw,
    OpenMeteoFloodRaw,
    UsgsEarthquakeRaw,
    UsgsNwisRaw,
    UsgsVolcanoRaw,
]


# (models whose Spark schema is all StringType)
ALL_STRING_CASES = [
    AvalancheOrgRaw,
    EonetEventRaw,
    GdacsEventRaw,
    IbtacsCycloneRaw,
    NifcFireRaw,
    OpenMeteoFloodRaw,
    UsgsEarthquakeRaw,
    UsgsNwisRaw,
    UsgsVolcanoRaw,
]


class TestRawRow:
    @pytest.mark.parametrize("model_cls", ALL_ACCEPTED_CASES)
    def test_all_fields_accepted(self, model_cls):
        # Arrange
        kwargs = VALID_ROWS[model_cls]
        # Act
        model = model_cls(**kwargs)
        # Assert
        for name, value in kwargs.items():
            assert getattr(model, name) == value

    @pytest.mark.parametrize("model_cls, expected", OPTIONAL_PRESENT_CASES)
    def test_optional_fields_present(self, model_cls, expected):
        # Act
        model = model_cls(**VALID_ROWS[model_cls])
        # Assert
        for field, value in expected.items():
            assert getattr(model, field) == value


class TestRawMissing:
    @pytest.mark.parametrize("model_cls, field", MISSING_KEY_CASES)
    def test_missing_required_field_raises(self, model_cls, field):
        # Arrange
        kwargs = {k: v for k, v in VALID_ROWS[model_cls].items() if k != field}
        # Assert
        with pytest.raises(ValidationError):
            model_cls(**kwargs)


class TestRawSparkSchema:
    @pytest.mark.parametrize("model_cls", ALL_STRING_CASES)
    def test_all_fields_are_string_type(self, model_cls):
        # Act
        schema = pydantic_to_spark_schema(model_cls)
        # Assert
        for field in schema.fields:
            assert isinstance(field.dataType, StringType), f"{field.name} is not StringType"

    def test_field_order_matches_download_csv_header(self):
        # Act
        schema = pydantic_to_spark_schema(NifcFireRaw)
        # Assert
        csv_fields = [field.name for field in schema.fields if field.name != "file_date"]
        assert csv_fields == CSV_HEADER_ORDER
