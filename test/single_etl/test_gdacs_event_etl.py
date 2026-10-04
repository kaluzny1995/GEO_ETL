import datetime as dt
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType

from model.enums import LoadType
from model.etl.gdacs_event_etl import GdacsEventEtl
from model.plain.gdacs_event import GdacsEvent
from single_etl.gdacs_event_etl import GdacsEventEtlScript
from utils.schema_converter import pydantic_to_spark_schema

_SCRIPT = GdacsEventEtlScript()


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
        "event_type": "TC",
        "event_id": 1001331,
        "event_name": "Tropical Cyclone NOLO-26",
        "alert_level": "Green",
        "country": None,
        "country_code": None,
        "latitude": 14.2,
        "longitude": -130.8,
        "from_date": dt.datetime(2026, 9, 29, 15, 0, 0),
        "to_date": dt.datetime(2026, 9, 29, 21, 0, 0),
        "date_modified": dt.datetime(2026, 9, 29, 21, 4, 58),
        "severity": 55.5552,
        "severity_text": "Tropical Depression (maximum wind speed of 56 km/h)",
        "severity_unit": "km/h",
        "source": "NOAA",
        "report_url": "https://www.gdacs.org/report.aspx?eventid=1001331&episodeid=2&eventtype=TC",
        "is_current": True,
        "geocode_country": None,
        "geocode_state": None,
        "file_date": "2026-09-29",
    }
    row.update(overrides)
    return Row(**row)


def _source_df(spark, *rows):
    schema = StructType([StructField(f.name, f.dataType, True) for f in pydantic_to_spark_schema(GdacsEvent).fields])
    return spark.createDataFrame(list(rows), schema=schema)


def _convert(spark, *rows):
    return _SCRIPT._convert(_source_df(spark, *rows)).collect()


class TestRun:
    @patch("single_etl.gdacs_event_etl.GdacsEventEtlScript._convert")
    @patch("utils.etl_utils.get_max_file_date", return_value=None)
    def test_auto_detects_historical(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()

        message = _SCRIPT.run(spark, _mock_config(), _mock_db_config())

        assert message.startswith("HISTORICAL")
        assert "wrote to geo.gdacs_event_etl" in message
        spark.read.jdbc.return_value.filter.assert_not_called()
        writer = mock_convert.return_value.write.jdbc
        writer.assert_called_once()
        assert writer.call_args.kwargs["table"] == "geo.gdacs_event_etl"
        assert writer.call_args.kwargs["mode"] == "append"

    @patch("single_etl.gdacs_event_etl.GdacsEventEtlScript._convert")
    @patch("utils.etl_utils.get_max_file_date", return_value=dt.date(2026, 9, 28))
    def test_auto_detects_incremental(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()

        message = _SCRIPT.run(spark, _mock_config(), _mock_db_config())

        assert message.startswith("INCREMENTAL")
        spark.read.jdbc.return_value.filter.assert_called_once()
        condition = str(spark.read.jdbc.return_value.filter.call_args[0][0])
        assert "file_date" in condition
        assert "2026-09-28" in condition
        mock_convert.return_value.write.jdbc.assert_called_once()

    @patch("single_etl.gdacs_event_etl.GdacsEventEtlScript._convert")
    @patch("utils.etl_utils.get_max_file_date", return_value=dt.date(2026, 9, 28))
    def test_forced_historical_skips_filter(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()

        message = _SCRIPT.run(spark, _mock_config(), _mock_db_config(), load_type=LoadType.HISTORICAL)

        assert message.startswith("HISTORICAL")
        spark.read.jdbc.return_value.filter.assert_not_called()
        mock_convert.return_value.write.jdbc.assert_called_once()

    @patch("single_etl.gdacs_event_etl.GdacsEventEtlScript._convert")
    @patch("utils.etl_utils.get_max_file_date", return_value=dt.date(2026, 9, 28))
    def test_incremental_without_new_rows_writes_nothing(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()
        spark.read.jdbc.return_value.filter.return_value.head.return_value = []

        message = _SCRIPT.run(spark, _mock_config(), _mock_db_config())

        assert message.startswith("INCREMENTAL")
        assert "nothing to do" in message
        mock_convert.assert_not_called()
        mock_convert.return_value.write.jdbc.assert_not_called()

    @patch("single_etl.gdacs_event_etl.GdacsEventEtlScript._convert")
    @patch("utils.etl_utils.get_max_file_date", return_value=dt.date(2026, 9, 28))
    def test_reads_source_table(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()

        _SCRIPT.run(spark, _mock_config(), _mock_db_config())

        assert spark.read.jdbc.call_args.kwargs["table"] == "geo.gdacs_event"
        assert mock_convert.call_args.args[0] is spark.read.jdbc.return_value.filter.return_value


class TestConvert:
    def test_column_order_matches_the_model(self, spark_session):
        df = _SCRIPT._convert(_source_df(spark_session, _source_row()))
        assert df.columns == list(GdacsEventEtl.model_fields)

    def test_cyclone_mapping(self, spark_session):
        (row,) = _convert(spark_session, _source_row())

        assert row.source_name == "GDACS"
        assert row.subsource_name == "NOAA"
        assert row.event_name == "Tropical Cyclone NOLO-26"
        assert row.event_category == "storm"
        assert row.event_subcategory == "tropical depression"
        assert row.event_description is None
        assert row.latitude == 14.2 and row.longitude == -130.8
        assert row.country_code == "ZZ"
        assert row.state_province is None
        assert row.place_location is None
        assert row.metric_name == "wind speed"
        assert row.metric_value == 55.5552
        assert row.metric_unit == "km/h"
        assert row.danger_rating == "Green"
        assert row.other_metrics is None
        assert row.event_start == dt.datetime(2026, 9, 29, 15, 0, 0)
        assert row.event_end == dt.datetime(2026, 9, 29, 21, 0, 0)
        assert row.last_latitude is None and row.last_longitude is None
        assert row.file_date == "2026-09-29"

    @pytest.mark.parametrize(
        ("severity_text", "expected_subcategory"),
        [
            ("Tropical Depression (maximum wind speed of 56 km/h)", "tropical depression"),
            ("Tropical Storm (maximum wind speed of 70 km/h)", "tropical storm"),
            ("Hurricane/Typhoon > 74 mph (maximum wind speed of 120 km/h)", "hurricane"),
        ],
    )
    def test_cyclone_intensity_subcategory(self, spark_session, severity_text, expected_subcategory):
        (row,) = _convert(spark_session, _source_row(severity_text=severity_text))

        assert row.event_category == "storm"
        assert row.event_subcategory == expected_subcategory

    def test_earthquake_mapping(self, spark_session):
        (row,) = _convert(
            spark_session,
            _source_row(
                event_type="EQ",
                event_name="Earthquake in Colombia",
                country="Colombia",
                country_code="COL",
                latitude=3.8311,
                longitude=-75.5906,
                from_date=dt.datetime(2026, 9, 29, 21, 50, 29),
                to_date=dt.datetime(2026, 9, 29, 21, 50, 29),
                severity=4.5,
                severity_text="Magnitude 4.5M, Depth:10km",
                severity_unit="M",
                source="NEIC",
            ),
        )

        assert row.event_category == "earthquake"
        assert row.event_subcategory is None
        assert row.subsource_name == "NEIC"
        assert row.country_code == "CO"
        assert row.state_province is None
        assert row.place_location == "Colombia"
        assert row.metric_name == "magnitude"
        assert row.metric_value == 4.5
        assert row.metric_unit == "M"
        assert row.danger_rating == "Green"
        assert row.event_start == row.event_end == dt.datetime(2026, 9, 29, 21, 50, 29)

    def test_wildfire_mapping(self, spark_session):
        (row,) = _convert(
            spark_session,
            _source_row(
                event_type="WF",
                event_name="Forest fires in Australia",
                country="Australia",
                country_code="AUS",
                severity=14196.0,
                severity_text="Green impact for forestfire in 14196 ha",
                severity_unit="ha",
                source="GWIS",
            ),
        )

        assert row.event_category == "fire"
        assert row.event_subcategory == "wildfire"
        assert row.country_code == "AU"
        assert row.place_location == "Australia"
        assert row.metric_name == "area"
        assert row.metric_value == 14196.0
        assert row.metric_unit == "ha"

    def test_drought_mapping(self, spark_session):
        (row,) = _convert(
            spark_session,
            _source_row(
                event_type="DR",
                event_name="Drought in Brazil, Colombia",
                country="Brazil, Colombia",
                country_code="BRA",
                from_date=dt.datetime(2026, 9, 1, 0, 0, 0),
                to_date=dt.datetime(2026, 9, 20, 0, 0, 0),
                severity=1264372.0,
                severity_text="Minor impact for agricultural drought in 1264372 km2",
                severity_unit="km2",
                source="GDO",
                alert_level="Orange",
            ),
        )

        assert row.event_category == "drought"
        assert row.event_subcategory == "agricultural drought"
        assert row.country_code == "BR"
        assert row.place_location == "Brazil, Colombia"
        assert row.metric_name == "area"
        assert row.metric_value == 1264372.0
        assert row.metric_unit == "km2"
        assert row.danger_rating == "Orange"

    @pytest.mark.parametrize(
        ("event_type", "severity_unit", "severity_text", "expected_category"),
        [
            ("FL", "", "Magnitude 0", "flood"),
            ("VO", None, None, "volcano"),
        ],
    )
    def test_undefined_severity_keeps_metric_value_and_unit_null(
        self, spark_session, event_type, severity_unit, severity_text, expected_category
    ):
        (row,) = _convert(
            spark_session,
            _source_row(
                event_type=event_type,
                event_name="Flood in India" if event_type == "FL" else "Eruption  Krakatau",
                country="India" if event_type == "FL" else "Indonesia",
                country_code="IND" if event_type == "FL" else "IDN",
                severity=0.0,
                severity_unit=severity_unit,
                severity_text=severity_text,
                source="GLOFAS" if event_type == "FL" else "TOKYO",
            ),
        )

        assert row.event_category == expected_category
        assert row.event_subcategory is None
        assert row.metric_name == "magnitude"
        assert row.metric_value is None
        assert row.metric_unit is None

    @pytest.mark.parametrize(
        ("country_code", "expected"),
        [("USA", "US"), ("DEU", "DE"), ("COD", "CD"), ("GLP", "GP"), ("TWN", "TW")],
    )
    def test_alpha3_country_code_is_translated_to_alpha2(self, spark_session, country_code, expected):
        (row,) = _convert(spark_session, _source_row(event_type="EQ", country_code=country_code))

        assert row.country_code == expected

    def test_blank_country_code_falls_back_to_the_stamped_geocode(self, spark_session):
        (row,) = _convert(spark_session, _source_row(geocode_country="PH", geocode_state=""))

        assert row.country_code == "PH"
        assert row.state_province is None

    def test_stamped_geocode_supplies_the_state_even_when_the_code_is_known(self, spark_session):
        (row,) = _convert(spark_session, _source_row(country_code="USA", geocode_country="US", geocode_state="TX"))

        assert row.country_code == "US"
        assert row.state_province == "TX"

    def test_unknown_country_code_resolves_from_the_stamped_geocode_then_falls_back(self, spark_session):
        (row,) = _convert(spark_session, _source_row(event_type="EQ", country_code="XXX"))

        assert row.country_code == "ZZ"

    def test_blank_country_leaves_place_null(self, spark_session):
        (row,) = _convert(spark_session, _source_row(event_type="EQ", country_code="NZL", country=None))

        assert row.place_location is None

    def test_unknown_event_type_is_lowercased(self, spark_session):
        (row,) = _convert(spark_session, _source_row(event_type="XX"))

        assert row.event_category == "xx"
        assert row.event_subcategory is None
        assert row.metric_name == "magnitude"

    def test_dates_come_from_the_from_to_window(self, spark_session):
        (row,) = _convert(
            spark_session,
            _source_row(from_date=dt.datetime(2026, 9, 1, 0, 0, 0), to_date=dt.datetime(2026, 9, 20, 0, 0, 0)),
        )

        assert row.event_start == dt.datetime(2026, 9, 1, 0, 0, 0)
        assert row.event_end == dt.datetime(2026, 9, 20, 0, 0, 0)
