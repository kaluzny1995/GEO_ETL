import datetime as dt
from unittest.mock import MagicMock, patch

import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType

from model.enums import LoadType
from model.etl.eonet_event_etl import EonetEventEtl
from model.plain.eonet_event import EonetEvent
from single_etl import eonet_event_etl
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
        "event_id": "EONET_1234",
        "event_name": "Hurricane Polo",
        "category_id": "severeStorms",
        "category_name": "Severe Storms",
        "description": None,
        "source_id": "EO",
        "source_url": "https://example.org/eonet/1234",
        "latitude": 15.1433,
        "longitude": -104.7983,
        "event_date": dt.datetime(2026, 9, 20, 12, 0, 0),
        "magnitude_value": 60.0,
        "magnitude_unit": "kts",
        "closed": None,
        "geometry_count": 36,
        "last_latitude": 16.0,
        "last_longitude": -105.0,
        "last_date": dt.datetime(2026, 9, 21, 12, 0, 0),
        "file_date": "2026-09-21",
    }
    row.update(overrides)
    return Row(**row)


def _source_df(spark, *rows):
    schema = StructType([StructField(f.name, f.dataType, True) for f in pydantic_to_spark_schema(EonetEvent).fields])
    return spark.createDataFrame(list(rows), schema=schema)


def _convert(spark, *rows):
    return eonet_event_etl._convert(_source_df(spark, *rows)).collect()


class TestResolveLoadType:
    def test_auto_empty_target_is_historical(self):
        assert eonet_event_etl._resolve_load_type(None, None) is LoadType.HISTORICAL

    def test_auto_populated_target_is_incremental(self):
        assert eonet_event_etl._resolve_load_type(dt.date(2026, 9, 28), None) is LoadType.INCREMENTAL

    def test_forced_historical_beats_populated_target(self):
        assert eonet_event_etl._resolve_load_type(dt.date(2026, 9, 28), LoadType.HISTORICAL) is LoadType.HISTORICAL

    def test_forced_incremental_on_empty_target_falls_back_to_historical(self):
        assert eonet_event_etl._resolve_load_type(None, LoadType.INCREMENTAL) is LoadType.HISTORICAL


class TestRun:
    @patch("single_etl.eonet_event_etl._convert")
    @patch("single_etl.eonet_event_etl.get_max_file_date", return_value=None)
    def test_auto_detects_historical(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()

        message = eonet_event_etl.run(spark, _mock_config(), _mock_db_config())

        assert message.startswith("HISTORICAL")
        assert "wrote to geo.eonet_event_etl" in message
        spark.read.jdbc.return_value.filter.assert_not_called()
        writer = mock_convert.return_value.write.jdbc
        writer.assert_called_once()
        assert writer.call_args.kwargs["table"] == "geo.eonet_event_etl"
        assert writer.call_args.kwargs["mode"] == "append"

    @patch("single_etl.eonet_event_etl._convert")
    @patch("single_etl.eonet_event_etl.get_max_file_date", return_value=dt.date(2026, 9, 28))
    def test_auto_detects_incremental(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()

        message = eonet_event_etl.run(spark, _mock_config(), _mock_db_config())

        assert message.startswith("INCREMENTAL")
        spark.read.jdbc.return_value.filter.assert_called_once()
        condition = str(spark.read.jdbc.return_value.filter.call_args[0][0])
        assert "file_date" in condition
        assert "2026-09-28" in condition
        mock_convert.return_value.write.jdbc.assert_called_once()

    @patch("single_etl.eonet_event_etl._convert")
    @patch("single_etl.eonet_event_etl.get_max_file_date", return_value=dt.date(2026, 9, 28))
    def test_forced_historical_skips_filter(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()

        message = eonet_event_etl.run(spark, _mock_config(), _mock_db_config(), load_type=LoadType.HISTORICAL)

        assert message.startswith("HISTORICAL")
        spark.read.jdbc.return_value.filter.assert_not_called()
        mock_convert.return_value.write.jdbc.assert_called_once()

    @patch("single_etl.eonet_event_etl._convert")
    @patch("single_etl.eonet_event_etl.get_max_file_date", return_value=dt.date(2026, 9, 28))
    def test_incremental_without_new_rows_writes_nothing(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()
        spark.read.jdbc.return_value.filter.return_value.head.return_value = []

        message = eonet_event_etl.run(spark, _mock_config(), _mock_db_config())

        assert message.startswith("INCREMENTAL")
        assert "nothing to do" in message
        mock_convert.assert_not_called()
        mock_convert.return_value.write.jdbc.assert_not_called()

    @patch("single_etl.eonet_event_etl._convert")
    @patch("single_etl.eonet_event_etl.get_max_file_date", return_value=dt.date(2026, 9, 28))
    def test_reads_source_table(self, mock_max, mock_convert, spark_session):
        spark = _spark_with_rows()

        eonet_event_etl.run(spark, _mock_config(), _mock_db_config())

        assert spark.read.jdbc.call_args.kwargs["table"] == "geo.eonet_event"
        assert mock_convert.call_args.args[0] is spark.read.jdbc.return_value.filter.return_value

    @patch("single_etl.eonet_event_etl.geocode_map")
    @patch("single_etl.eonet_event_etl.get_max_file_date", return_value=None)
    def test_unbuildable_geocode_fails_the_run(self, mock_max, mock_geocode, spark_session):
        mock_geocode.side_effect = RuntimeError("reverse geocode failed")
        spark = _spark_with_rows()

        with pytest.raises(RuntimeError, match="reverse geocode"):
            eonet_event_etl.run(spark, _mock_config(), _mock_db_config())


class TestGeocodeMap:
    @patch("single_etl.eonet_event_etl.geocode_map")
    def test_resolves_each_distinct_coordinate(self, mock_geocode, spark_session):
        mock_geocode.return_value = {"15.1433,-104.7983": ("", ""), "1.0000,2.0000": ("US", "TX")}
        df = _source_df(
            spark_session,
            _source_row(),
            _source_row(file_date="2026-09-22", last_latitude=17.0),
            _source_row(latitude=1.0, longitude=2.0),
        )

        mapping = eonet_event_etl._geocode_map(df)

        assert mapping == {(15.1433, -104.7983): ("", ""), (1.0, 2.0): ("US", "TX")}
        assert set(mock_geocode.call_args.args[0]) == {(15.1433, -104.7983), (1.0, 2.0)}
        assert mock_geocode.call_args.kwargs["output"] == eonet_event_etl.GEOCODE_FILE

    @patch("single_etl.eonet_event_etl.geocode_map")
    def test_null_coordinates_are_skipped(self, mock_geocode, spark_session):
        mock_geocode.return_value = {}
        df = _source_df(spark_session, _source_row(latitude=None, longitude=None))

        assert eonet_event_etl._geocode_map(df) == {}
        assert set(mock_geocode.call_args.args[0]) == set()


class TestConvert:
    @pytest.fixture(autouse=True)
    def _geocode(self, monkeypatch):
        monkeypatch.setattr(eonet_event_etl, "_geocode_map", lambda *args, **kwargs: {})

    def test_column_order_matches_the_model(self, spark_session):
        df = eonet_event_etl._convert(_source_df(spark_session, _source_row()))
        assert df.columns == list(EonetEventEtl.model_fields)

    def test_storm_mapping(self, spark_session):
        (row,) = _convert(spark_session, _source_row())

        assert row.source_name == "EONET"
        assert row.subsource_name == "EO"
        assert row.event_name == "Hurricane Polo"
        assert row.event_category == "storm"
        assert row.event_subcategory == "hurricane"
        assert row.event_description is None
        assert row.latitude == 15.1433 and row.longitude == -104.7983
        assert row.country_code == "ZZ"
        assert row.state_province is None
        assert row.place_location is None
        assert row.metric_name == "wind speed"
        assert row.metric_value == 60.0
        assert row.metric_unit == "kts"
        assert row.danger_rating is None
        assert row.statement is None
        assert row.other_metrics == '{"geometry_count":36}'
        assert row.event_start_date == dt.date(2026, 9, 20)
        assert row.event_start == dt.datetime(2026, 9, 20, 12, 0, 0)
        assert row.event_end == dt.datetime(2026, 9, 21, 12, 0, 0)
        assert row.last_latitude == 16.0 and row.last_longitude == -105.0
        assert row.file_date == "2026-09-21"

    @pytest.mark.parametrize(
        ("name", "expected_place"),
        [
            ("Wildfire Snow, Custer, Montana", "Snow, Custer, Montana"),
            ("Prescribed Fire RX CALV BU 104-108, Vernon, Louisiana", "RX CALV BU 104-108, Vernon, Louisiana"),
            ("Incident Complex Rattlesnake Complex, Prairie, Montana", "Rattlesnake Complex, Prairie, Montana"),
            (
                "Emergency Stabilization BAER McConnell, Humboldt, Nevada",
                "McConnell, Humboldt, Nevada",
            ),
            ("Hurricane Polo", None),
            ("Iceberg D32", None),
        ],
    )
    def test_place_location_extraction(self, spark_session, name, expected_place):
        (row,) = _convert(spark_session, _source_row(event_name=name))

        assert row.place_location == expected_place

    def test_fire_maps_category_state_and_metric_from_the_description(self, spark_session):
        (row,) = _convert(
            spark_session,
            _source_row(
                event_name="Wildfire Snow, Custer, Montana",
                category_id="wildfires",
                category_name="Wildfires",
                description="30 Miles E from Miles City, MT",
                source_id="IRWIN",
                magnitude_value=120.0,
                magnitude_unit="acres",
            ),
        )

        assert row.event_category == "fire"
        assert row.event_subcategory == "wildfire"
        assert row.place_location == "Snow, Custer, Montana"
        assert row.country_code == "US"
        assert row.state_province == "MT"
        assert row.metric_name == "area"
        assert row.metric_value == 120.0
        assert row.metric_unit == "acres"

    def test_state_and_country_from_the_geocode_when_the_description_has_none(self, monkeypatch, spark_session):
        monkeypatch.setattr(
            eonet_event_etl,
            "_geocode_map",
            lambda *args, **kwargs: {(46.586036, -104.927967): ("US", "MT")},
        )

        (row,) = _convert(spark_session, _source_row(latitude=46.586036, longitude=-104.927967))

        assert row.country_code == "US"
        assert row.state_province == "MT"

    def test_the_description_state_beats_the_geocode(self, monkeypatch, spark_session):
        monkeypatch.setattr(
            eonet_event_etl,
            "_geocode_map",
            lambda *args, **kwargs: {(15.1433, -104.7983): ("MX", "GRO")},
        )

        (row,) = _convert(spark_session, _source_row(description="3 Miles E from Meridian, TX"))

        assert row.country_code == "US"
        assert row.state_province == "TX"

    def test_geocoded_country_without_subdivision_leaves_state_null(self, monkeypatch, spark_session):
        monkeypatch.setattr(
            eonet_event_etl,
            "_geocode_map",
            lambda *args, **kwargs: {(15.1433, -104.7983): ("PH", "")},
        )

        (row,) = _convert(spark_session, _source_row())

        assert row.country_code == "PH"
        assert row.state_province is None

    def test_sea_and_lake_ice_event(self, spark_session):
        (row,) = _convert(
            spark_session,
            _source_row(
                event_name="Iceberg B22A",
                category_id="seaLakeIce",
                category_name="Sea and Lake Ice",
                magnitude_value=5.0,
                magnitude_unit="NM^2",
            ),
        )

        assert row.event_category == "iceberg"
        assert row.event_subcategory == "sea_lake_ice"
        assert row.metric_name == "surface"
        assert row.metric_value == 5.0
        assert row.metric_unit == "NM^2"
        assert row.place_location is None

    def test_storm_without_a_type_keyword_has_no_subcategory(self, spark_session):
        (row,) = _convert(spark_session, _source_row(event_name="Storm System 42"))

        assert row.event_category == "storm"
        assert row.event_subcategory is None

    def test_tropical_cyclone_storm_subcategory(self, spark_session):
        (row,) = _convert(spark_session, _source_row(event_name="Tropical Cyclone 01B"))

        assert row.event_subcategory == "tropical cyclone"

    def test_unknown_category_is_snake_cased(self, spark_session):
        (row,) = _convert(
            spark_session,
            _source_row(event_name="Volcano X", category_id="volcanicAsh", category_name="Volcanic Ash"),
        )

        assert row.event_category == "volcanic_ash"
        assert row.event_subcategory is None
        assert row.metric_name == "magnitude"

    def test_blank_description_and_unit_stay_null(self, spark_session):
        (row,) = _convert(spark_session, _source_row(description="   ", magnitude_unit=""))

        assert row.event_description is None
        assert row.metric_unit is None

    def test_missing_geometry_count_keeps_other_metrics_null(self, spark_session):
        (row,) = _convert(spark_session, _source_row(geometry_count=None))

        assert row.other_metrics is None
