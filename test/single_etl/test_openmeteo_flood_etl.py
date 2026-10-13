import csv
import datetime as dt
import json

import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructField, StructType

from model.etl.openmeteo_flood_etl import OpenMeteoFloodEtl
from model.plain.openmeteo_flood import OpenMeteoFlood
from single_etl import openmeteo_flood_etl as module
from single_etl.openmeteo_flood_etl import OpenMeteoFloodEtlScript
from utils.schema_converter import pydantic_to_spark_schema

_NO_THRESHOLDS = "/nonexistent/openmeteo_discharge_thresholds.csv"

_SCRIPT = OpenMeteoFloodEtlScript(thresholds_path=_NO_THRESHOLDS)

_THRESHOLD_FIELDS = [
    "river_name",
    "country",
    "latitude",
    "longitude",
    "baseline_days",
    "baseline_start",
    "baseline_end",
    "discharge_p50_m3s",
    "discharge_p90_m3s",
    "discharge_p95_m3s",
    "discharge_p99_m3s",
]

_THRESHOLD_ROW = {
    "river_name": "Amazon",
    "country": "Brazil",
    "latitude": -3.14,
    "longitude": -59.98,
    "baseline_days": 10870,
    "baseline_start": "1997-01-01",
    "baseline_end": "2026-10-05",
    "discharge_p50_m3s": 133044.0,
    "discharge_p90_m3s": 203235.0,
    "discharge_p95_m3s": 215774.0,
    "discharge_p99_m3s": 226985.0,
}


def _source_row(**overrides):
    row = {
        "river_name": "Amazon",
        "country": "Brazil",
        "latitude": -3.14,
        "longitude": -59.98,
        "date": dt.date(2026, 9, 1),
        "discharge_m3s": 77982.05,
        "discharge_mean_m3s": None,
        "discharge_median_m3s": None,
        "discharge_p25_m3s": None,
        "discharge_p75_m3s": None,
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


def _thresholds_csv(tmp_path, **overrides):
    """Write a one-river threshold reference and return its path."""
    row = {**_THRESHOLD_ROW, **overrides}
    path = tmp_path / "openmeteo_discharge_thresholds.csv"
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=_THRESHOLD_FIELDS)
        writer.writeheader()
        writer.writerow(row)
    return str(path)


def _convert(spark, *rows, script=None):
    return (script or _SCRIPT)._convert(_source_df(spark, *rows)).collect()


class TestConvert:
    def test_column_order_matches_the_model(self, spark_session):
        df = _SCRIPT._convert(_source_df(spark_session, _source_row()))
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
        assert row.danger_rating == "no_rating"
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


class TestThresholdReference:
    def test_below_the_threshold_rates_none_and_still_reports_the_ratio(self, spark_session, tmp_path):
        script = OpenMeteoFloodEtlScript(thresholds_path=_thresholds_csv(tmp_path))

        (row,) = _convert(spark_session, _source_row(), script=script)
        metrics = json.loads(row.other_metrics)

        assert row.danger_rating == "none"
        assert "exceedance_level" not in metrics
        assert metrics["threshold_percentile"] == 0.95
        assert metrics["threshold"] == 215774.0
        assert metrics["threshold_unit"] == "m³/s"
        assert metrics["threshold_exceeded"] is False
        assert metrics["exceedance_ratio"] == pytest.approx(77982.05 / 215774.0)
        assert metrics["discharge_p50"] == 133044.0
        assert metrics["discharge_p90"] == 203235.0
        assert metrics["discharge_p99"] == 226985.0
        assert {metrics[f"discharge_p{level}_unit"] for level in (50, 90, 99)} == {"m³/s"}
        assert metrics["baseline_days"] == 10870
        assert metrics["baseline_start"] == "1997-01-01"
        assert metrics["baseline_end"] == "2026-10-05"
        assert "ensemble_mean" not in metrics
        assert not any(key.endswith("_m3s") for key in metrics)

    @pytest.mark.parametrize(
        ("discharge_m3s", "expected_rating", "expected_exceeded", "expected_level"),
        [
            (215774.0, "none", False, None),  # exactly the configured threshold — not exceeded
            (215775.0, "high", True, "Q95"),
            (226985.0, "high", True, "Q95"),  # exactly Q99 — not exceeded there, so the ladder stops at Q95
            (226986.0, "extreme", True, "Q99"),
            (215773.0, "none", False, None),
        ],
    )
    def test_the_rating_is_the_highest_level_the_day_exceeded(
        self, spark_session, tmp_path, discharge_m3s, expected_rating, expected_exceeded, expected_level
    ):
        script = OpenMeteoFloodEtlScript(thresholds_path=_thresholds_csv(tmp_path))

        (row,) = _convert(spark_session, _source_row(discharge_m3s=discharge_m3s), script=script)
        metrics = json.loads(row.other_metrics)

        assert row.danger_rating == expected_rating
        assert metrics["threshold_exceeded"] is expected_exceeded
        assert metrics.get("exceedance_level") == expected_level

    def test_a_river_the_reference_does_not_cover_rates_no_rating(self, spark_session, tmp_path):
        script = OpenMeteoFloodEtlScript(thresholds_path=_thresholds_csv(tmp_path))

        (row,) = _convert(spark_session, _source_row(country="Atlantis"), script=script)

        assert row.danger_rating == "no_rating"
        assert row.other_metrics is None

    def test_a_rated_river_without_a_discharge_rates_no_rating(self, spark_session, tmp_path):
        script = OpenMeteoFloodEtlScript(thresholds_path=_thresholds_csv(tmp_path))

        (row,) = _convert(spark_session, _source_row(discharge_m3s=None), script=script)

        assert row.danger_rating == "no_rating"

    def test_a_different_country_with_the_same_river_name_is_a_different_river(self, spark_session, tmp_path):
        script = OpenMeteoFloodEtlScript(thresholds_path=_thresholds_csv(tmp_path))

        (row,) = _convert(spark_session, _source_row(country="Peru"), script=script)

        assert row.other_metrics is None

    def test_the_default_path_is_the_file_name_from_the_config(self):
        script = OpenMeteoFloodEtlScript()

        assert script.thresholds_path.endswith("openmeteo_discharge_thresholds.csv")
        assert script.thresholds_path != _NO_THRESHOLDS

    def test_a_threshold_level_the_reference_does_not_carry_is_rejected(self, monkeypatch):
        monkeypatch.setattr(module, "_cfg", module._cfg.model_copy(update={"threshold_percentile": 0.999}))

        with pytest.raises(ValueError, match="has no column in the threshold reference"):
            module._other_metrics()

    def test_a_missing_percentile_omits_its_value_and_its_unit(self, spark_session, tmp_path):
        script = OpenMeteoFloodEtlScript(thresholds_path=_thresholds_csv(tmp_path, discharge_p50_m3s=""))

        (row,) = _convert(spark_session, _source_row(), script=script)
        metrics = json.loads(row.other_metrics)

        assert "discharge_p50" not in metrics
        assert "discharge_p50_unit" not in metrics
        assert metrics["discharge_p90"] == 203235.0
        assert metrics["discharge_p90_unit"] == "m³/s"


class TestEnsembleMetrics:
    def test_forecast_statistics_land_in_other_metrics(self, spark_session):
        (row,) = _convert(
            spark_session,
            _source_row(
                discharge_mean_m3s=55094.71,
                discharge_median_m3s=55079.8,
                discharge_p25_m3s=55079.8,
                discharge_p75_m3s=55206.76,
            ),
        )
        metrics = json.loads(row.other_metrics)

        assert metrics == {
            "ensemble_mean": 55094.71,
            "ensemble_mean_unit": "m³/s",
            "ensemble_median": 55079.8,
            "ensemble_median_unit": "m³/s",
            "ensemble_p25": 55079.8,
            "ensemble_p25_unit": "m³/s",
            "ensemble_p75": 55206.76,
            "ensemble_p75_unit": "m³/s",
        }

    @pytest.mark.parametrize(
        "overrides",
        [
            {"discharge_mean_m3s": None, "discharge_p25_m3s": 55079.8},
            {
                "discharge_mean_m3s": 55094.71,
                "discharge_median_m3s": None,
                "discharge_p25_m3s": None,
                "discharge_p75_m3s": None,
            },
        ],
    )
    def test_partial_ensemble_answers_omit_the_missing_keys(self, spark_session, overrides):
        (row,) = _convert(spark_session, _source_row(**overrides))
        metrics = json.loads(row.other_metrics)

        statistics = [key for key, value in overrides.items() if value is not None]
        expected = {
            f"ensemble_{stat.removeprefix('discharge_').removesuffix('_m3s')}{suffix}"
            for stat in statistics
            for suffix in ("", "_unit")
        }
        assert set(metrics) == expected

    def test_a_day_without_threshold_or_ensemble_has_no_other_metrics(self, spark_session, tmp_path):
        script = OpenMeteoFloodEtlScript(thresholds_path=_thresholds_csv(tmp_path, river_name="Nile", country="Egypt"))

        (row,) = _convert(spark_session, _source_row(), script=script)

        assert row.other_metrics is None
