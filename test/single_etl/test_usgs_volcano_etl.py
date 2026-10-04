import datetime as dt
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType

from model.etl.usgs_volcano_etl import UsgsVolcanoEtl
from model.plain.usgs_volcano import UsgsVolcano
from single_etl import usgs_volcano_etl
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
        "volcano_name": "Great Sitkin",
        "vnum": 311120,
        "observatory": "avo",
        "observatory_name": "Alaska Volcano Observatory",
        "alert_level": "WATCH",
        "color_code": "ORANGE",
        "notice_type": "DU",
        "notice_identifier": "DOI-USGS-AVO-2026-09-29T18:26:19+00:00",
        "notice_date": dt.datetime(2026, 9, 29, 18, 26, 19),
        "notice_url": "https://volcanoes.usgs.gov/hans-public/notice/DOI-USGS-AVO-2026-09-29T18:26:19+00:00",
        "notice_data_url": "https://volcanoes.usgs.gov/hans-public/api/notice/getNotice/DOI-USGS-AVO-2026-09-29T18:26:19+00:00",
        "notice_type_name": "Daily Update",
        "latitude": 52.0765,
        "longitude": -176.1109,
        "region": "Aleutians",
        "synopsis": "AVO Great Sitkin ORANGE/WATCH - Slow eruption of lava continues.",
        "geocode_state": None,
        "file_date": "2026-09-29",
    }
    row.update(overrides)
    return Row(**row)


def _source_df(spark, *rows):
    schema = StructType([StructField(f.name, f.dataType, True) for f in pydantic_to_spark_schema(UsgsVolcano).fields])
    return spark.createDataFrame(list(rows), schema=schema)


def _convert(spark, *rows):
    return usgs_volcano_etl._convert(_source_df(spark, *rows)).collect()


class TestRun:
    @patch("single_etl.usgs_volcano_etl._convert")
    @patch("utils.etl_utils.get_max_file_date", return_value=None)
    def test_auto_detects_historical(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()

        message = usgs_volcano_etl.run(spark, _mock_config(), _mock_db_config())

        assert message.startswith("HISTORICAL")
        spark.read.jdbc.return_value.filter.assert_not_called()
        mock_convert.return_value.write.jdbc.assert_called_once()

    @patch("single_etl.usgs_volcano_etl._convert")
    @patch("utils.etl_utils.get_max_file_date", return_value=dt.date(2026, 9, 29))
    def test_incremental_without_new_rows_writes_nothing(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()
        spark.read.jdbc.return_value.filter.return_value.head.return_value = []

        message = usgs_volcano_etl.run(spark, _mock_config(), _mock_db_config())

        assert message.startswith("INCREMENTAL")
        assert "nothing to do" in message
        mock_convert.assert_not_called()
        mock_convert.return_value.write.jdbc.assert_not_called()

    @patch("single_etl.usgs_volcano_etl._convert")
    @patch("utils.etl_utils.get_max_file_date", return_value=dt.date(2026, 9, 29))
    def test_reads_source_table(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()

        usgs_volcano_etl.run(spark, _mock_config(), _mock_db_config())

        assert spark.read.jdbc.call_args.kwargs["table"] == "geo.usgs_volcano"
        assert mock_convert.call_args.args[0] is spark.read.jdbc.return_value.filter.return_value


class TestConvert:
    def test_column_order_matches_the_model(self, spark_session):
        df = usgs_volcano_etl._convert(_source_df(spark_session, _source_row()))
        assert df.columns == list(UsgsVolcanoEtl.model_fields)

    def test_event_mapping(self, spark_session):
        (row,) = _convert(spark_session, _source_row())

        assert row.source_name == "USGS"
        assert row.subsource_name == "Alaska Volcano Observatory"
        assert row.event_name == "Volcano - Great Sitkin"
        assert row.event_category == "volcano"
        assert row.event_subcategory == "Daily Update"
        assert row.event_description == "AVO Great Sitkin ORANGE/WATCH - Slow eruption of lava continues."
        assert row.latitude == 52.0765
        assert row.longitude == -176.1109
        assert row.country_code == "US"
        assert row.state_province == "ZZ"
        assert row.place_location == "Great Sitkin, Aleutians"
        assert row.metric_name == "alert_level"
        assert row.metric_value == 2.0
        assert row.metric_unit is None
        assert row.danger_rating == "WATCH"
        assert row.other_metrics is None
        assert row.event_start == dt.datetime(2026, 9, 29, 18, 26, 19)
        assert row.event_end is None
        assert row.last_latitude is None and row.last_longitude is None
        assert row.file_date == "2026-09-29"

    @pytest.mark.parametrize(
        ("alert_level", "expected_value"),
        [
            ("NORMAL", 0.0),
            ("ADVISORY", 1.0),
            ("WATCH", 2.0),
            ("WARNING", 3.0),
            ("watch", 2.0),
            ("UNASSIGNED", None),
            ("", None),
            (None, None),
        ],
    )
    def test_metric_value_is_the_alert_scale(self, spark_session, alert_level, expected_value):
        (row,) = _convert(spark_session, _source_row(alert_level=alert_level))

        assert row.metric_name == "alert_level"
        assert row.metric_value == expected_value
        assert row.danger_rating == alert_level

    @pytest.mark.parametrize(
        ("geocode_state", "expected_state"),
        [
            ("AK", "AK"),
            ("", "ZZ"),
            (None, "ZZ"),
        ],
    )
    def test_state_province_is_the_stamped_geocode_with_zz_fallback(self, spark_session, geocode_state, expected_state):
        (row,) = _convert(spark_session, _source_row(geocode_state=geocode_state))

        assert row.state_province == expected_state
        assert row.country_code == "US"

    def test_a_notice_without_coordinates_keeps_the_zz_fallback(self, spark_session):
        (row,) = _convert(spark_session, _source_row(latitude=None, longitude=None))

        assert row.latitude is None and row.longitude is None
        assert row.state_province == "ZZ"

    @pytest.mark.parametrize(
        ("notice_type_name", "expected_subcategory"),
        [
            ("Daily Update", "Daily Update"),
            ("Volcano Activity Notification", "Volcano Activity Notification"),
            ("", "DU"),
            (None, "DU"),
        ],
    )
    def test_subcategory_is_the_full_notice_type_with_code_fallback(
        self, spark_session, notice_type_name, expected_subcategory
    ):
        (row,) = _convert(spark_session, _source_row(notice_type_name=notice_type_name))

        assert row.event_subcategory == expected_subcategory

    @pytest.mark.parametrize(
        ("overrides", "expected_place"),
        [
            ({}, "Great Sitkin, Aleutians"),
            ({"region": ""}, "Great Sitkin"),
            ({"region": None}, "Great Sitkin"),
        ],
    )
    def test_place_is_volcano_and_region(self, spark_session, overrides, expected_place):
        (row,) = _convert(spark_session, _source_row(**overrides))

        assert row.place_location == expected_place

    @pytest.mark.parametrize("volcano_name", [None, "", "   "])
    def test_blank_volcano_name_falls_back(self, spark_session, volcano_name):
        (row,) = _convert(spark_session, _source_row(volcano_name=volcano_name))

        assert row.event_name == "Volcano"
        assert row.place_location is None

    @pytest.mark.parametrize("synopsis", [None, "", "   "])
    def test_blank_synopsis_has_no_description(self, spark_session, synopsis):
        (row,) = _convert(spark_session, _source_row(synopsis=synopsis))

        assert row.event_description is None

    @pytest.mark.parametrize("notice_type_name", [None, "", "   "])
    def test_blank_notice_type_name_falls_back_to_the_code(self, spark_session, notice_type_name):
        (row,) = _convert(spark_session, _source_row(notice_type_name=notice_type_name))

        assert row.event_subcategory == "DU"

    def test_missing_notice_details_keep_the_feed_fields(self, spark_session):
        (row,) = _convert(
            spark_session,
            _source_row(notice_type_name=None, latitude=None, longitude=None, region=None, synopsis=None),
        )

        assert row.event_subcategory == "DU"
        assert row.latitude is None and row.longitude is None
        assert row.state_province == "ZZ"
        assert row.event_description is None
        assert row.place_location == "Great Sitkin"
        assert row.metric_value == 2.0
        assert row.danger_rating == "WATCH"
