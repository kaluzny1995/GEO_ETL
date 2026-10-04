import datetime as dt
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType

from model.etl.usgs_earthquake_etl import UsgsEarthquakeEtl
from model.plain.usgs_earthquake import UsgsEarthquake
from single_etl.usgs_earthquake_etl import UsgsEarthquakeEtlScript
from utils.schema_converter import pydantic_to_spark_schema

_SCRIPT = UsgsEarthquakeEtlScript()


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
        "time": dt.datetime(2026, 9, 1, 23, 58, 46, 50000),
        "latitude": 33.704166666667,
        "longitude": -116.80866666667,
        "depth": 15.95,
        "mag": 0.52,
        "mag_type": "ml",
        "nst": 15,
        "gap": 149,
        "dmin": 0.07908,
        "rms": 0.08,
        "net": "ci",
        "event_id": "ci41540448",
        "updated": dt.datetime(2026, 9, 4, 19, 13, 3, 582000),
        "place": "9 km WSW of Idyllwild, CA",
        "event_type": "earthquake",
        "horizontal_error": 0.29,
        "depth_error": 0.48,
        "mag_error": 0.077,
        "mag_nst": 6,
        "status": "reviewed",
        "location_source": "ci",
        "mag_source": "ci",
        "geocode_country": None,
        "geocode_state": None,
        "file_date": "2026-09-01",
    }
    row.update(overrides)
    return Row(**row)


def _source_df(spark, *rows):
    schema = StructType(
        [StructField(f.name, f.dataType, True) for f in pydantic_to_spark_schema(UsgsEarthquake).fields]
    )
    return spark.createDataFrame(list(rows), schema=schema)


def _convert(spark, *rows):
    return _SCRIPT._convert(_source_df(spark, *rows)).collect()


class TestRun:
    @patch("single_etl.usgs_earthquake_etl.UsgsEarthquakeEtlScript._convert")
    @patch("utils.etl_utils.get_max_file_date", return_value=None)
    def test_auto_detects_historical(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()

        message = _SCRIPT.run(spark, _mock_config(), _mock_db_config())

        assert message.startswith("HISTORICAL")
        spark.read.jdbc.return_value.filter.assert_not_called()
        mock_convert.return_value.write.jdbc.assert_called_once()

    @patch("single_etl.usgs_earthquake_etl.UsgsEarthquakeEtlScript._convert")
    @patch("utils.etl_utils.get_max_file_date", return_value=dt.date(2026, 9, 28))
    def test_incremental_without_new_rows_writes_nothing(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()
        spark.read.jdbc.return_value.filter.return_value.head.return_value = []

        message = _SCRIPT.run(spark, _mock_config(), _mock_db_config())

        assert message.startswith("INCREMENTAL")
        assert "nothing to do" in message
        mock_convert.assert_not_called()
        mock_convert.return_value.write.jdbc.assert_not_called()

    @patch("single_etl.usgs_earthquake_etl.UsgsEarthquakeEtlScript._convert")
    @patch("utils.etl_utils.get_max_file_date", return_value=dt.date(2026, 9, 28))
    def test_reads_source_table(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()

        _SCRIPT.run(spark, _mock_config(), _mock_db_config())

        assert spark.read.jdbc.call_args.kwargs["table"] == "geo.usgs_earthquake"
        assert mock_convert.call_args.args[0] is spark.read.jdbc.return_value.filter.return_value


class TestConvert:
    def test_column_order_matches_the_model(self, spark_session):
        df = _SCRIPT._convert(_source_df(spark_session, _source_row()))
        assert df.columns == list(UsgsEarthquakeEtl.model_fields)

    def test_event_mapping(self, spark_session):
        (row,) = _convert(spark_session, _source_row())

        assert row.source_name == "USGS"
        assert row.subsource_name == "ci"
        assert row.event_name == "Earthquake - United States"
        assert row.event_category == "earthquake"
        assert row.event_subcategory == "earthquake"
        assert row.event_description == "earthquake reviewed"
        assert row.latitude == 33.704166666667
        assert row.longitude == -116.80866666667
        assert row.country_code == "US"
        assert row.state_province == "CI"
        assert row.place_location == "9 km WSW of Idyllwild, CA"
        assert row.metric_name == "magnitude"
        assert row.metric_value == 0.52
        assert row.metric_unit == "ml"
        assert row.danger_rating is None
        assert row.other_metrics == '{"depth":15.95,"depth_unit":"km","nst":15,"nst_unit":"stations"}'
        assert row.event_start == dt.datetime(2026, 9, 1, 23, 58, 46, 50000)
        assert row.event_end is None
        assert row.last_latitude is None and row.last_longitude is None
        assert row.file_date == "2026-09-01"

    @pytest.mark.parametrize(
        "place",
        ["9 km WSW of Idyllwild, CA", "12 km SW of Falls City, Texas"],
    )
    def test_us_place_suffix_is_the_united_states(self, spark_session, place):
        (row,) = _convert(spark_session, _source_row(place=place))

        assert row.country_code == "US"
        assert row.event_name == "Earthquake - United States"

    @pytest.mark.parametrize(
        ("place", "expected_code"),
        [
            ("105 km N of Vallenar, Chile", "CL"),
            ("3 km SW of Acapulco, MX", "MX"),
            ("8 km E of Hualien City, Taiwan", "TW"),
        ],
    )
    def test_international_suffix_maps_to_the_iso_code(self, spark_session, place, expected_code):
        (row,) = _convert(spark_session, _source_row(place=place))

        assert row.country_code == expected_code
        assert row.event_name == f"Earthquake - {place.rsplit(', ', 1)[-1]}"

    def test_unknown_suffix_falls_back_to_the_stamped_geocode(self, spark_session):
        (row,) = _convert(
            spark_session,
            _source_row(place="3 km NE of Nowhere, Atlantis", geocode_country="JP", geocode_state=""),
        )

        assert row.country_code == "JP"
        assert row.event_name == "Earthquake - Japan"

    def test_unknown_suffix_without_a_stamped_geocode_is_zz(self, spark_session):
        (row,) = _convert(spark_session, _source_row(place="3 km NE of Nowhere, Atlantis"))

        assert row.country_code == "ZZ"
        assert row.event_name == "Earthquake"

    @pytest.mark.parametrize("net", ["ak", "us", "nc"])
    def test_state_province_is_the_uppercased_network(self, spark_session, net):
        (row,) = _convert(spark_session, _source_row(net=net))

        assert row.state_province == net.upper()
        assert row.subsource_name == net

    def test_blank_net_falls_back_to_the_stamped_geocode_state(self, spark_session):
        (row,) = _convert(spark_session, _source_row(net=None, geocode_state="CA"))

        assert row.state_province == "CA"

    def test_blank_net_without_a_stamped_geocode_state_is_null(self, spark_session):
        (row,) = _convert(spark_session, _source_row(net=None))

        assert row.state_province is None

    @pytest.mark.parametrize(
        ("event_type", "status", "expected_description"),
        [
            ("earthquake", "reviewed", "earthquake reviewed"),
            ("quarry blast", "automatic", "quarry blast automatic"),
            ("explosion", "reviewed", "explosion reviewed"),
        ],
    )
    def test_description_is_event_type_and_status(self, spark_session, event_type, status, expected_description):
        (row,) = _convert(spark_session, _source_row(event_type=event_type, status=status))

        assert row.event_description == expected_description
        assert row.event_subcategory == event_type

    @pytest.mark.parametrize("status", [None, "", "   "])
    def test_blank_status_drops_that_part_of_the_description(self, spark_session, status):
        (row,) = _convert(spark_session, _source_row(status=status))

        assert row.event_description == "earthquake"

    @pytest.mark.parametrize(
        ("nst", "expected_json"),
        [
            (15, '{"depth":15.95,"depth_unit":"km","nst":15,"nst_unit":"stations"}'),
            (None, '{"depth":15.95,"depth_unit":"km"}'),
        ],
    )
    def test_other_metrics_carry_depth_and_station_count_with_units(self, spark_session, nst, expected_json):
        (row,) = _convert(spark_session, _source_row(nst=nst))

        assert row.other_metrics == expected_json

    @pytest.mark.parametrize(
        ("mag", "expected_value"),
        [
            (0.52, 0.52),
            (None, 0.0),
        ],
    )
    def test_metric_value_is_the_magnitude(self, spark_session, mag, expected_value):
        (row,) = _convert(spark_session, _source_row(mag=mag))

        assert row.metric_name == "magnitude"
        assert row.metric_value == expected_value
        assert row.metric_unit == "ml"
