import datetime as dt
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType

from model.enums import LoadType
from model.etl.openmeteo_flood_etl import OpenMeteoFloodEtl
from model.plain.openmeteo_flood import OpenMeteoFlood
from single_etl import openmeteo_flood_etl
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
        "river_name": "Amazon",
        "country": "Brazil",
        "latitude": -3.14,
        "longitude": -59.98,
        "date": dt.date(2026, 9, 1),
        "discharge_m3s": 77982.05,
        "geocode_country": None,
        "geocode_state": None,
        "file_date": "2026-09-01",
    }
    row.update(overrides)
    return Row(**row)


def _source_df(spark, *rows):
    schema = StructType(
        [StructField(f.name, f.dataType, True) for f in pydantic_to_spark_schema(OpenMeteoFlood).fields]
    )
    return spark.createDataFrame(list(rows), schema=schema)


def _convert(spark, *rows):
    return openmeteo_flood_etl._convert(_source_df(spark, *rows)).collect()


class TestResolveLoadType:
    def test_auto_empty_target_is_historical(self):
        assert openmeteo_flood_etl._resolve_load_type(None, None) is LoadType.HISTORICAL

    def test_auto_populated_target_is_incremental(self):
        assert openmeteo_flood_etl._resolve_load_type(dt.date(2026, 9, 28), None) is LoadType.INCREMENTAL

    def test_forced_historical_beats_populated_target(self):
        assert openmeteo_flood_etl._resolve_load_type(dt.date(2026, 9, 28), LoadType.HISTORICAL) is LoadType.HISTORICAL

    def test_forced_incremental_on_empty_target_falls_back_to_historical(self):
        assert openmeteo_flood_etl._resolve_load_type(None, LoadType.INCREMENTAL) is LoadType.HISTORICAL


class TestRun:
    @patch("single_etl.openmeteo_flood_etl._convert")
    @patch("single_etl.openmeteo_flood_etl.get_max_file_date", return_value=None)
    def test_auto_detects_historical(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()

        message = openmeteo_flood_etl.run(spark, _mock_config(), _mock_db_config())

        assert message.startswith("HISTORICAL")
        spark.read.jdbc.return_value.filter.assert_not_called()
        mock_convert.return_value.write.jdbc.assert_called_once()

    @patch("single_etl.openmeteo_flood_etl._convert")
    @patch("single_etl.openmeteo_flood_etl.get_max_file_date", return_value=dt.date(2026, 9, 28))
    def test_incremental_without_new_rows_writes_nothing(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()
        spark.read.jdbc.return_value.filter.return_value.head.return_value = []

        message = openmeteo_flood_etl.run(spark, _mock_config(), _mock_db_config())

        assert message.startswith("INCREMENTAL")
        assert "nothing to do" in message
        mock_convert.assert_not_called()
        mock_convert.return_value.write.jdbc.assert_not_called()

    @patch("single_etl.openmeteo_flood_etl._convert")
    @patch("single_etl.openmeteo_flood_etl.get_max_file_date", return_value=dt.date(2026, 9, 28))
    def test_reads_source_table(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()

        openmeteo_flood_etl.run(spark, _mock_config(), _mock_db_config())

        assert spark.read.jdbc.call_args.kwargs["table"] == "geo.openmeteo_flood"
        assert mock_convert.call_args.args[0] is spark.read.jdbc.return_value.filter.return_value


class TestConvert:
    def test_column_order_matches_the_model(self, spark_session):
        df = openmeteo_flood_etl._convert(_source_df(spark_session, _source_row()))
        assert df.columns == list(OpenMeteoFloodEtl.model_fields)

    def test_river_mapping(self, spark_session):
        (row,) = _convert(spark_session, _source_row())

        assert row.source_name == "OPENMETEO"
        assert row.subsource_name == "GLOFAS"
        assert row.event_name == "Amazon discharge"
        assert row.event_category == "flood"
        assert row.event_subcategory is None
        assert row.event_description is None
        assert row.latitude == -3.14
        assert row.longitude == -59.98
        assert row.country_code == "BR"
        assert row.state_province is None
        assert row.place_location == "Amazon - Brazil"
        assert row.metric_name == "discharge"
        assert row.metric_value == 77982.05
        assert row.metric_unit == "m³/s"
        assert row.danger_rating is None
        assert row.other_metrics is None
        assert row.event_start == dt.datetime(2026, 9, 1, 0, 0)
        assert row.event_end is None
        assert row.last_latitude is None and row.last_longitude is None
        assert row.file_date == "2026-09-01"

    @pytest.mark.parametrize(
        ("country", "expected_code"),
        [
            ("Brazil", "BR"),
            ("USA", "US"),
            ("UK", "GB"),
            ("DRC", "CD"),
            ("Papua New Guinea", "PG"),
        ],
    )
    def test_country_name_maps_to_the_iso_code(self, spark_session, country, expected_code):
        (row,) = _convert(spark_session, _source_row(country=country))

        assert row.country_code == expected_code

    def test_known_country_name_wins_over_the_stamped_geocode(self, spark_session):
        (row,) = _convert(spark_session, _source_row(country="Brazil", geocode_country="AR", geocode_state="C"))

        assert row.country_code == "BR"

    def test_unknown_country_name_falls_back_to_the_stamped_geocode(self, spark_session):
        (row,) = _convert(spark_session, _source_row(country="Atlantis", geocode_country="AR", geocode_state="C"))

        assert row.country_code == "AR"

    def test_unknown_country_without_a_stamped_geocode_is_zz(self, spark_session):
        (row,) = _convert(spark_session, _source_row(country="Atlantis"))

        assert row.country_code == "ZZ"

    def test_state_province_comes_from_the_stamped_geocode(self, spark_session):
        (row,) = _convert(spark_session, _source_row(geocode_country="BR", geocode_state="AM"))

        assert row.state_province == "AM"

    @pytest.mark.parametrize("state", ["", None])
    def test_missing_stamped_geocode_state_is_null(self, spark_session, state):
        (row,) = _convert(spark_session, _source_row(geocode_country="BR", geocode_state=state))

        assert row.state_province is None

    @pytest.mark.parametrize("river_name", [None, "", "   "])
    def test_blank_river_name_is_null(self, spark_session, river_name):
        (row,) = _convert(spark_session, _source_row(river_name=river_name))

        assert row.event_name is None

    @pytest.mark.parametrize("country", [None, "", "   "])
    def test_blank_country_drops_that_part_of_the_place(self, spark_session, country):
        (row,) = _convert(spark_session, _source_row(country=country))

        assert row.place_location == "Amazon"

    @pytest.mark.parametrize("river_name", [None, "", "   "])
    def test_blank_river_keeps_the_country_as_the_place(self, spark_session, river_name):
        (row,) = _convert(spark_session, _source_row(river_name=river_name))

        assert row.place_location == "Brazil"

    def test_both_place_parts_blank_is_null(self, spark_session):
        (row,) = _convert(spark_session, _source_row(river_name=None, country=None))

        assert row.place_location is None

    @pytest.mark.parametrize(
        ("discharge_m3s", "expected_value"),
        [
            (77982.05, 77982.05),
            (0.0, 0.0),
            (None, 0.0),
        ],
    )
    def test_metric_value_is_the_discharge(self, spark_session, discharge_m3s, expected_value):
        (row,) = _convert(spark_session, _source_row(discharge_m3s=discharge_m3s))

        assert row.metric_name == "discharge"
        assert row.metric_value == expected_value
        assert row.metric_unit == "m³/s"
