import datetime as dt
from unittest.mock import MagicMock, patch

from pyspark.sql import Row

from model.enums import LoadType
from model.etl.avalanche_org_etl import AvalancheOrgEtl
from single_etl.avalanche_org_etl import AvalancheOrgEtlScript

_SCRIPT = AvalancheOrgEtlScript()


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
        "zone_id": 3004,
        "zone_name": "Bridgeport",
        "center_id": "BAC",
        "center_name": "Bridgeport Avalanche Center",
        "state": "CA",
        "danger_level": 4,
        "danger_rating": "high",
        "travel_advice": "Very dangerous avalanche conditions.",
        "off_season": False,
        "forecast_start": dt.datetime(2026, 1, 10, 6, 0, 0),
        "forecast_end": dt.datetime(2026, 1, 11, 6, 0, 0),
        "warning": "",
        "link": "https://example.org/zone",
        "latitude": 38.5,
        "longitude": -119.5,
        "date": dt.date(2026, 1, 10),
        "file_date": "2026-01-10",
    }
    row.update(overrides)
    return Row(**row)


def _convert(spark, *rows):
    return _SCRIPT._convert(spark.createDataFrame(list(rows))).collect()


class TestRun:
    @patch("single_etl.avalanche_org_etl.AvalancheOrgEtlScript._convert")
    @patch("utils.etl_utils.get_max_file_date", return_value=None)
    def test_auto_detects_historical(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()

        message = _SCRIPT.run(spark, _mock_config(), _mock_db_config())

        assert message.startswith("HISTORICAL")
        assert "wrote to geo.avalanche_org_etl" in message
        spark.read.jdbc.return_value.filter.assert_not_called()
        writer = mock_convert.return_value.write.jdbc
        writer.assert_called_once()
        assert writer.call_args.kwargs["table"] == "geo.avalanche_org_etl"
        assert writer.call_args.kwargs["mode"] == "append"

    @patch("single_etl.avalanche_org_etl.AvalancheOrgEtlScript._convert")
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

    @patch("single_etl.avalanche_org_etl.AvalancheOrgEtlScript._convert")
    @patch("utils.etl_utils.get_max_file_date", return_value=dt.date(2026, 9, 28))
    def test_forced_historical_skips_filter(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()

        message = _SCRIPT.run(spark, _mock_config(), _mock_db_config(), load_type=LoadType.HISTORICAL)

        assert message.startswith("HISTORICAL")
        spark.read.jdbc.return_value.filter.assert_not_called()
        mock_convert.return_value.write.jdbc.assert_called_once()

    @patch("single_etl.avalanche_org_etl.AvalancheOrgEtlScript._convert")
    @patch("utils.etl_utils.get_max_file_date", return_value=dt.date(2026, 9, 28))
    def test_incremental_without_new_rows_writes_nothing(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()
        spark.read.jdbc.return_value.filter.return_value.head.return_value = []

        message = _SCRIPT.run(spark, _mock_config(), _mock_db_config())

        assert message.startswith("INCREMENTAL")
        assert "nothing to do" in message
        mock_convert.assert_not_called()
        mock_convert.return_value.write.jdbc.assert_not_called()

    @patch("single_etl.avalanche_org_etl.AvalancheOrgEtlScript._convert")
    @patch("utils.etl_utils.get_max_file_date", return_value=dt.date(2026, 9, 28))
    def test_reads_source_table(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()

        _SCRIPT.run(spark, _mock_config(), _mock_db_config())

        assert spark.read.jdbc.call_args.kwargs["table"] == "geo.avalanche_org"
        assert mock_convert.call_args.args[0] is spark.read.jdbc.return_value.filter.return_value


class TestConvert:
    def test_column_order_matches_the_model(self, spark_session):
        df = _SCRIPT._convert(spark_session.createDataFrame([_source_row()]))
        assert df.columns == list(AvalancheOrgEtl.model_fields)

    def test_zone_mapping(self, spark_session):
        (row,) = _convert(spark_session, _source_row())

        assert row.source_name == "AVALANCHE_ORG"
        assert row.subsource_name == "Bridgeport Avalanche Center"
        assert row.event_name is None
        assert row.place_location == "Bridgeport"
        assert row.event_category == "avalanche"
        assert row.event_subcategory is None
        assert row.event_description is None
        assert row.latitude == 38.5 and row.longitude == -119.5
        assert row.country_code == "US"
        assert row.state_province == "CA"
        assert row.metric_name == "magnitude"
        assert row.metric_value is None
        assert row.metric_unit is None
        assert row.danger_rating == "high"
        assert row.other_metrics is None
        assert row.event_start == dt.datetime(2026, 1, 10, 6, 0, 0)
        assert row.event_end == dt.datetime(2026, 1, 11, 6, 0, 0)
        assert row.last_latitude is None and row.last_longitude is None
        assert row.file_date == "2026-01-10"

    def test_unrated_zone_gets_null_rating(self, spark_session):
        (row,) = _convert(spark_session, _source_row(danger_level=-1, danger_rating="no rating"))

        assert row.danger_rating is None
        assert row.metric_value is None

    def test_unrated_zone_with_underscored_rating_gets_null(self, spark_session):
        (row,) = _convert(spark_session, _source_row(danger_level=-1, danger_rating="no_rating"))

        assert row.danger_rating is None

    def test_blank_rating_gets_null(self, spark_session):
        (row,) = _convert(spark_session, _source_row(danger_level=-1, danger_rating=""))

        assert row.danger_rating is None

    def test_row_without_source_coordinates_gets_null_coordinates(self, spark_session):
        schema = spark_session.createDataFrame([_source_row()]).schema
        df = spark_session.createDataFrame([_source_row(latitude=None, longitude=None)], schema=schema)
        (row,) = _SCRIPT._convert(df).collect()

        assert row.latitude is None
        assert row.longitude is None

    def test_source_coordinates_pass_through_unchanged(self, spark_session):
        (row,) = _convert(spark_session, _source_row(latitude=43.75, longitude=-110.5))

        assert row.latitude == 43.75
        assert row.longitude == -110.5

    def test_analysis_day_and_next_day_rows_collapse_to_one(self, spark_session):
        rows = [
            _source_row(date=dt.date(2026, 1, 10)),
            _source_row(date=dt.date(2026, 1, 11)),
        ]

        out = _convert(spark_session, *rows)

        assert len(out) == 1
        assert out[0].file_date == "2026-01-10"

    def test_rows_with_different_forecast_window_are_kept(self, spark_session):
        rows = [
            _source_row(),
            _source_row(
                date=dt.date(2026, 1, 11),
                forecast_start=dt.datetime(2026, 1, 11, 6, 0, 0),
                forecast_end=dt.datetime(2026, 1, 12, 6, 0, 0),
            ),
        ]

        out = _convert(spark_session, *rows)

        assert len(out) == 2
        assert {row.event_start for row in out} == {
            dt.datetime(2026, 1, 10, 6, 0, 0),
            dt.datetime(2026, 1, 11, 6, 0, 0),
        }
