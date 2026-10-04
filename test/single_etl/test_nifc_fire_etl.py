import datetime as dt
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType

from model.enums import LoadType
from model.etl.nifc_fire_etl import NifcFireEtl
from model.plain.nifc_fire import NifcFire
from single_etl import nifc_fire_etl
from utils.schema_converter import pydantic_to_spark_schema


def _mock_config():
    cfg = MagicMock()
    cfg.database.db_schema = "geo"
    cfg.database.jdbc_driver = "org.postgresql.Driver"
    return cfg


def _mock_db_config():
    cfg = MagicMock()
    cfg.get_jdbc_url.return_value = "jdbc:postgresql://db.example.com:5432/postgres"
    cfg.get_jdbc_properties.return_value = {"user": "user", "password": "pass"}
    return cfg


def _spark_with_rows():
    spark = MagicMock()
    spark.read.jdbc.return_value.head.return_value = [MagicMock()]
    return spark


def _source_row(**overrides):
    row = {
        "incident_name": "Iron Fire",
        "irwin_id": "{B22F7798-E3C3-4E1B-A091-CC85EBE4DA75}",
        "fire_number": None,
        "acres": 1500.5,
        "percent_contained": 45.0,
        "fire_type": "WF",
        "fire_type_label": "Wildfire Daily Fire Perimeter",
        "jurisdiction": "USFS",
        "jurisdiction_label": "WAOWF",
        "unit_name": "Okanogan-Wenatchee National Forest",
        "state": "UT",
        "po_method": "US-UT",
        "latitude": 40.017760846821886,
        "longitude": -112.14269529210874,
        "poly_date": dt.datetime(2026, 9, 1, 12, 1, 2),
        "created_on": dt.datetime(2026, 8, 10, 17, 1, 9),
        "modified_on": dt.datetime(2026, 9, 15, 8, 0, 0),
        "geocode_state": None,
        "file_date": "2026-09-01",
    }
    row.update(overrides)
    return Row(**row)


def _source_df(spark, *rows):
    schema = StructType([StructField(f.name, f.dataType, True) for f in pydantic_to_spark_schema(NifcFire).fields])
    return spark.createDataFrame(list(rows), schema=schema)


def _convert(spark, *rows):
    return nifc_fire_etl._convert(_source_df(spark, *rows)).collect()


class TestResolveLoadType:
    def test_auto_empty_target_is_historical(self):
        assert nifc_fire_etl._resolve_load_type(None, None) is LoadType.HISTORICAL

    def test_auto_populated_target_is_incremental(self):
        assert nifc_fire_etl._resolve_load_type(dt.date(2026, 9, 28), None) is LoadType.INCREMENTAL

    def test_forced_historical_beats_populated_target(self):
        assert nifc_fire_etl._resolve_load_type(dt.date(2026, 9, 28), LoadType.HISTORICAL) is LoadType.HISTORICAL

    def test_forced_incremental_on_empty_target_falls_back_to_historical(self):
        assert nifc_fire_etl._resolve_load_type(None, LoadType.INCREMENTAL) is LoadType.HISTORICAL


class TestRun:
    @patch("single_etl.nifc_fire_etl._convert")
    @patch("single_etl.nifc_fire_etl.get_max_file_date", return_value=None)
    def test_auto_detects_historical(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()

        message = nifc_fire_etl.run(spark, _mock_config(), _mock_db_config())

        assert message.startswith("HISTORICAL")
        spark.read.jdbc.return_value.filter.assert_not_called()
        mock_convert.return_value.write.jdbc.assert_called_once()

    @patch("single_etl.nifc_fire_etl._convert")
    @patch("single_etl.nifc_fire_etl.get_max_file_date", return_value=dt.date(2026, 9, 28))
    def test_incremental_without_new_rows_writes_nothing(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()
        spark.read.jdbc.return_value.filter.return_value.head.return_value = []

        message = nifc_fire_etl.run(spark, _mock_config(), _mock_db_config())

        assert message.startswith("INCREMENTAL")
        assert "nothing to do" in message
        mock_convert.assert_not_called()
        mock_convert.return_value.write.jdbc.assert_not_called()

    @patch("single_etl.nifc_fire_etl._convert")
    @patch("single_etl.nifc_fire_etl.get_max_file_date", return_value=dt.date(2026, 9, 28))
    def test_reads_source_table(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()

        nifc_fire_etl.run(spark, _mock_config(), _mock_db_config())

        assert spark.read.jdbc.call_args.kwargs["table"] == "geo.nifc_fire"
        assert mock_convert.call_args.args[0] is spark.read.jdbc.return_value.filter.return_value


class TestConvert:
    def test_column_order_matches_the_model(self, spark_session):
        df = nifc_fire_etl._convert(_source_df(spark_session, _source_row()))
        assert df.columns == list(NifcFireEtl.model_fields)

    def test_fire_mapping(self, spark_session):
        (row,) = _convert(spark_session, _source_row())

        assert row.source_name == "NIFC"
        assert row.subsource_name == "WFIGS"
        assert row.event_name == "Iron Fire"
        assert row.event_category == "fire"
        assert row.event_subcategory == "wildfire"
        assert row.event_description is None
        assert row.latitude == 40.017760846821886
        assert row.longitude == -112.14269529210874
        assert row.country_code == "US"
        assert row.state_province == "UT"
        assert row.place_location == "Okanogan-Wenatchee National Forest"
        assert row.metric_name == "area"
        assert row.metric_value == 1500.5
        assert row.metric_unit == "acres"
        assert row.danger_rating is None
        assert row.other_metrics == '{"percent_contained":45,"percent_contained_unit":"%"}'
        assert row.event_start == dt.datetime(2026, 8, 10, 17, 1, 9)
        assert row.event_end is None
        assert row.last_latitude is None and row.last_longitude is None
        assert row.file_date == "2026-09-01"

    @pytest.mark.parametrize(
        ("fire_type", "expected_subcategory"),
        [
            ("WF", "wildfire"),
            ("RX", "prescribed fire"),
            (None, None),
            ("", None),
            ("XX", None),
        ],
    )
    def test_subcategory_comes_from_fire_type(self, spark_session, fire_type, expected_subcategory):
        (row,) = _convert(spark_session, _source_row(fire_type=fire_type))

        assert row.event_subcategory == expected_subcategory

    @pytest.mark.parametrize("incident_name", [None, "", "   "])
    def test_blank_incident_name_is_null(self, spark_session, incident_name):
        (row,) = _convert(spark_session, _source_row(incident_name=incident_name))

        assert row.event_name is None

    def test_source_state_wins_over_the_stamped_geocode(self, spark_session):
        (row,) = _convert(spark_session, _source_row(state="UT", geocode_state="NM"))

        assert row.state_province == "UT"

    @pytest.mark.parametrize("state", [None, "", "   "])
    def test_blank_state_falls_back_to_the_stamped_geocode(self, spark_session, state):
        (row,) = _convert(spark_session, _source_row(state=state, geocode_state="NM"))

        assert row.state_province == "NM"

    def test_blank_state_without_a_stamped_geocode_is_null(self, spark_session):
        (row,) = _convert(spark_session, _source_row(state=None))

        assert row.state_province is None

    def test_unit_name_passes_through_unchanged(self, spark_session):
        (row,) = _convert(spark_session, _source_row(jurisdiction_label="NVNV0100", unit_name="Okanogan NF"))

        assert row.place_location == "Okanogan NF"

    def test_stamped_raw_code_passes_through(self, spark_session):
        (row,) = _convert(spark_session, _source_row(jurisdiction_label="NVNV0100", unit_name="NVNV0100"))

        assert row.place_location == "NVNV0100"

    @pytest.mark.parametrize("unit_name", [None, "", "   "])
    def test_blank_unit_name_is_null(self, spark_session, unit_name):
        (row,) = _convert(spark_session, _source_row(unit_name=unit_name))

        assert row.place_location is None

    @pytest.mark.parametrize(
        ("percent_contained", "expected_json"),
        [
            (93.0, '{"percent_contained":93,"percent_contained_unit":"%"}'),
            (None, '{"percent_contained":0,"percent_contained_unit":"%"}'),
        ],
    )
    def test_other_metrics_always_report_containment_with_its_unit(
        self, spark_session, percent_contained, expected_json
    ):
        (row,) = _convert(spark_session, _source_row(percent_contained=percent_contained))

        assert row.other_metrics == expected_json

    @pytest.mark.parametrize(
        ("acres", "expected_value"),
        [
            (1500.5, 1500.5),
            (None, 0.0),
        ],
    )
    def test_metric_value_is_the_acreage(self, spark_session, acres, expected_value):
        (row,) = _convert(spark_session, _source_row(acres=acres))

        assert row.metric_name == "area"
        assert row.metric_value == expected_value
        assert row.metric_unit == "acres"
