import datetime as dt
import json

import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType

from model.etl.ibtracs_cyclone_etl import IbtracsCycloneEtl
from model.plain.ibtracs_cyclone import IbtacsCyclone
from single_etl.ibtracs_cyclone_etl import IbtracsCycloneEtlScript
from utils.schema_converter import pydantic_to_spark_schema

_SCRIPT = IbtracsCycloneEtlScript()


def _source_row(**overrides):
    row = {
        "sid": "AL092026",
        "season": 2026,
        "number": 9,
        "basin": "NA",
        "subbasin": "GM",
        "name": "EDOUARD",
        "iso_time": dt.datetime(2026, 9, 1, 21, 0, 0),
        "nature": "TS",
        "lat": 29.9,
        "lon": -93.9,
        "wmo_wind": None,
        "wmo_pres": None,
        "wmo_agency": None,
        "track_type": "PROVISIONAL",
        "dist2land": 0,
        "landfall": 0,
        "iflag": "OFCL",
        "usa_agency": "nhc_working_bt",
        "usa_atcf_id": "AL092026",
        "usa_lat": 29.9,
        "usa_lon": -93.9,
        "usa_record": None,
        "usa_status": "HU",
        "usa_wind": 65,
        "usa_pres": 985,
        "usa_sshs": 1,
        "usa_r34_ne": None,
        "usa_r34_se": None,
        "usa_r34_sw": None,
        "usa_r34_nw": None,
        "usa_r50_ne": None,
        "usa_r50_se": None,
        "usa_r50_sw": None,
        "usa_r50_nw": None,
        "usa_r64_ne": None,
        "usa_r64_se": None,
        "usa_r64_sw": None,
        "usa_r64_nw": None,
        "usa_poci": None,
        "usa_roci": None,
        "usa_rmw": None,
        "usa_eye": None,
        "storm_speed": 8,
        "storm_dir": 280,
        "usa_gust": None,
        "geocode_country": None,
        "geocode_state": None,
        "file_date": "2026-09-01",
    }
    row.update(overrides)
    return Row(**row)


def _source_df(spark, *rows):
    schema = StructType([StructField(f.name, f.dataType, True) for f in pydantic_to_spark_schema(IbtacsCyclone).fields])
    return spark.createDataFrame(list(rows), schema=schema)


def _convert(spark, *rows):
    return _SCRIPT._convert(_source_df(spark, *rows)).collect()


class TestConvert:
    def test_column_order_matches_the_model(self, spark_session):
        df = _SCRIPT._convert(_source_df(spark_session, _source_row()))
        assert df.columns == list(IbtracsCycloneEtl.model_fields)

    def test_position_mapping(self, spark_session):
        (row,) = _convert(spark_session, _source_row())

        assert row.source_name == "IBTRACS"
        assert row.subsource_name == "NA"
        assert row.event_name == "EDOUARD"
        assert row.event_category == "storm"
        assert row.event_subcategory == "hurricane"
        assert row.event_description is None
        assert row.latitude == 29.9 and row.longitude == -93.9
        assert row.country_code == "ZZ"
        assert row.state_province is None
        assert row.place_location == "North Atlantic - Gulf of Mexico"
        assert row.metric_name == "wind speed"
        assert row.metric_value == 120.38000000000001
        assert row.metric_unit == "km/h"
        assert row.danger_rating == "medium"
        assert row.event_start == dt.datetime(2026, 9, 1, 21, 0, 0)
        assert row.event_end == dt.datetime(2026, 9, 1, 21, 0, 0)
        assert row.last_latitude == 29.9 and row.last_longitude == -93.9
        assert row.file_date == "2026-09-01"

    @pytest.mark.parametrize(
        ("usa_status", "expected_subcategory"),
        [
            ("HU", "hurricane"),
            ("TY", "typhoon"),
            ("TS", "tropical storm"),
            ("TD", "tropical depression"),
            ("DB", "disturbance"),
            ("LO", "low"),
        ],
    )
    def test_status_drives_the_subcategory(self, spark_session, usa_status, expected_subcategory):
        (row,) = _convert(spark_session, _source_row(usa_status=usa_status))

        assert row.event_subcategory == expected_subcategory

    @pytest.mark.parametrize(
        ("nature", "expected_subcategory"),
        [
            ("TS", "tropical"),
            ("DS", "disturbance"),
            ("ET", "extratropical"),
            ("SS", "subtropical"),
        ],
    )
    def test_blank_status_falls_back_to_nature(self, spark_session, nature, expected_subcategory):
        (row,) = _convert(spark_session, _source_row(usa_status=" ", nature=nature))

        assert row.event_subcategory == expected_subcategory

    def test_blank_status_and_unreported_nature_stay_null(self, spark_session):
        (row,) = _convert(spark_session, _source_row(usa_status=" ", nature="NR"))

        assert row.event_subcategory is None

    @pytest.mark.parametrize(
        ("basin", "subbasin", "expected_place"),
        [
            ("NA", "GM", "North Atlantic - Gulf of Mexico"),
            ("NA", "NA", "North Atlantic"),
            ("EP", "CP", "Eastern North Pacific - Central Pacific"),
            ("EP", "MM", "Eastern North Pacific"),
            ("WP", "MM", "Western North Pacific"),
            ("NI", "BB", "North Indian - Bay of Bengal"),
        ],
    )
    def test_place_is_the_full_basin_name_extended_with_the_subbasin(
        self, spark_session, basin, subbasin, expected_place
    ):
        (row,) = _convert(spark_session, _source_row(basin=basin, subbasin=subbasin))

        assert row.subsource_name == basin
        assert row.place_location == expected_place

    @pytest.mark.parametrize(
        ("usa_sshs", "expected_rating"),
        [
            (5, "extreme"),
            (4, "extreme"),
            (3, "high"),
            (2, "medium"),
            (1, "medium"),
            (0, "low"),
            (-1, "low"),
            (-3, "low"),
            (None, "no_rating"),
            (-5, "no_rating"),
            (9, "no_rating"),
        ],
    )
    def test_danger_rating_is_the_saffir_simpson_class_on_the_shared_scale(
        self, spark_session, usa_sshs, expected_rating
    ):
        (row,) = _convert(spark_session, _source_row(usa_sshs=usa_sshs))

        assert row.danger_rating == expected_rating

    def test_the_source_keeps_its_own_saffir_simpson_wording_in_other_metrics(self, spark_session):
        (row,) = _convert(spark_session, _source_row(usa_sshs=1))

        assert json.loads(row.other_metrics)["saffir_simpson"] == "category 1"

    def test_wmo_wind_wins_over_the_us_agency_value(self, spark_session):
        (row,) = _convert(spark_session, _source_row(wmo_wind=70, usa_wind=65))

        assert row.metric_value == 129.64000000000001

    def test_us_wind_is_used_when_the_wmo_value_is_absent(self, spark_session):
        (row,) = _convert(spark_session, _source_row(wmo_wind=None, usa_wind=65))

        assert row.metric_value == 120.38000000000001

    def test_no_wind_at_all_keeps_the_metric_value_null(self, spark_session):
        (row,) = _convert(spark_session, _source_row(wmo_wind=None, usa_wind=None))

        assert row.metric_value is None
        assert row.metric_unit == "km/h"

    def test_other_metrics_carry_the_six_measurements_with_their_units(self, spark_session):
        (row,) = _convert(spark_session, _source_row(usa_gust=130))

        metrics = json.loads(row.other_metrics)
        assert metrics["saffir_simpson"] == "category 1"
        assert metrics["pressure"] == 985 and metrics["pressure_unit"] == "mb"
        assert metrics["speed"] == pytest.approx(8 * 1.852) and metrics["speed_unit"] == "km/h"
        assert metrics["direction"] == 280 and metrics["direction_unit"] == "degrees"
        assert metrics["gust"] == pytest.approx(130 * 1.852) and metrics["gust_unit"] == "km/h"
        assert metrics["dist2land"] == 0 and metrics["dist2land_unit"] == "km"
        assert metrics["landfall"] == 0 and metrics["landfall_unit"] == "km"

    def test_absent_measurements_are_omitted_with_their_units(self, spark_session):
        (row,) = _convert(spark_session, _source_row(usa_pres=None, usa_gust=None))

        metrics = json.loads(row.other_metrics)
        assert "pressure" not in metrics and "pressure_unit" not in metrics
        assert "gust" not in metrics and "gust_unit" not in metrics
        assert metrics["speed_unit"] == "km/h"
        assert metrics["dist2land_unit"] == "km"

    def test_other_metrics_omit_the_absent_measurements(self, spark_session):
        (row,) = _convert(
            spark_session,
            _source_row(usa_pres=None, storm_speed=None, storm_dir=None, usa_gust=None, dist2land=None, landfall=None),
        )

        assert json.loads(row.other_metrics) == {"saffir_simpson": "category 1"}

    def test_other_metrics_are_null_when_the_point_reports_nothing(self, spark_session):
        (row,) = _convert(
            spark_session,
            _source_row(
                usa_sshs=None,
                usa_pres=None,
                storm_speed=None,
                storm_dir=None,
                usa_gust=None,
                dist2land=None,
                landfall=None,
            ),
        )

        assert row.other_metrics is None

    def test_latest_position_is_the_storms_last_point(self, spark_session):
        rows = _convert(
            spark_session,
            _source_row(iso_time=dt.datetime(2026, 9, 1, 0, 0, 0), lat=25.0, lon=-90.0, file_date="2026-09-01"),
            _source_row(iso_time=dt.datetime(2026, 9, 1, 6, 0, 0), lat=26.5, lon=-91.0, file_date="2026-09-01"),
            _source_row(
                sid="AL102026",
                name="FREDA",
                iso_time=dt.datetime(2026, 9, 1, 6, 0, 0),
                lat=14.0,
                lon=-40.0,
                basin="NA",
                subbasin="NA",
                file_date="2026-09-01",
            ),
        )

        by_time = {(row.event_start, row.event_name): row for row in rows}
        first_point = by_time[(dt.datetime(2026, 9, 1, 0, 0, 0), "EDOUARD")]
        second_point = by_time[(dt.datetime(2026, 9, 1, 6, 0, 0), "EDOUARD")]
        other_storm = by_time[(dt.datetime(2026, 9, 1, 6, 0, 0), "FREDA")]

        assert (first_point.last_latitude, first_point.last_longitude) == (26.5, -91.0)
        assert (second_point.last_latitude, second_point.last_longitude) == (26.5, -91.0)
        assert (other_storm.last_latitude, other_storm.last_longitude) == (14.0, -40.0)

    def test_blank_name_leaves_the_event_name_null(self, spark_session):
        (row,) = _convert(spark_session, _source_row(name=" "))

        assert row.event_name is None

    def test_stamped_geocode_supplies_country_and_state(self, spark_session):
        (row,) = _convert(spark_session, _source_row(geocode_country="US", geocode_state="TX"))

        assert row.country_code == "US"
        assert row.state_province == "TX"

    def test_ocean_point_resolves_to_zz_without_a_state(self, spark_session):
        (row,) = _convert(spark_session, _source_row(geocode_country=None, geocode_state=None))

        assert row.country_code == "ZZ"
        assert row.state_province is None
