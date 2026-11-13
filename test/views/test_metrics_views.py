"""The eight ``metrics_*_vw`` scripts — observed plus predicted rows.

All eight views share one contract and differ only in their phenomenon category,
so every assertion is parametrized over the category/script pair.
"""

import pytest

import jobs
from factory import ViewScriptFactory
from views import (
    metrics_avalanche_vw,
    metrics_drought_vw,
    metrics_earthquake_vw,
    metrics_fire_vw,
    metrics_flood_vw,
    metrics_iceberg_vw,
    metrics_storm_vw,
    metrics_volcano_vw,
)

CASES = [
    ("avalanche", metrics_avalanche_vw.MetricsAvalancheVwScript, metrics_avalanche_vw._cfg),
    ("drought", metrics_drought_vw.MetricsDroughtVwScript, metrics_drought_vw._cfg),
    ("earthquake", metrics_earthquake_vw.MetricsEarthquakeVwScript, metrics_earthquake_vw._cfg),
    ("fire", metrics_fire_vw.MetricsFireVwScript, metrics_fire_vw._cfg),
    ("flood", metrics_flood_vw.MetricsFloodVwScript, metrics_flood_vw._cfg),
    ("iceberg", metrics_iceberg_vw.MetricsIcebergVwScript, metrics_iceberg_vw._cfg),
    ("storm", metrics_storm_vw.MetricsStormVwScript, metrics_storm_vw._cfg),
    ("volcano", metrics_volcano_vw.MetricsVolcanoVwScript, metrics_volcano_vw._cfg),
]
IDS = [category for category, _, _ in CASES]


@pytest.mark.parametrize("category, script_cls, cfg", CASES, ids=IDS)
def test_source_is_the_shared_observed_view(category, script_cls, cfg):
    assert script_cls.SOURCE_TABLE == "metrics_observed_vw"
    assert cfg.source_table == "metrics_observed_vw"


@pytest.mark.parametrize("category, script_cls, cfg", CASES, ids=IDS)
def test_target_view_comes_from_the_config_and_ends_with_vw(category, script_cls, cfg):
    expected = f"metrics_{category}_vw"
    assert expected == script_cls.TARGET_VIEW
    assert cfg.target_view == script_cls.TARGET_VIEW
    assert script_cls.TARGET_VIEW.endswith("_vw")


@pytest.mark.parametrize("category, script_cls, cfg", CASES, ids=IDS)
def test_query_comes_from_the_config(category, script_cls, cfg):
    assert cfg.query == script_cls.QUERY


@pytest.mark.parametrize("category, script_cls, cfg", CASES, ids=IDS)
def test_query_reads_observed_and_predicted_rows(category, script_cls, cfg):
    query = script_cls.QUERY

    assert "FROM geo.metrics_observed_vw" in query
    assert "FROM geo.prediction_etl" in query
    assert "predicted_value AS metric_value" in query
    assert "is_predicted" in query
    assert query.count(f"WHERE event_category = '{category}'") == 2
    for column in ("event_category", "event_subcategory", "metric_name", "metric_value", "metric_unit", "file_date"):
        assert column in query


@pytest.mark.parametrize("category, script_cls, cfg", CASES, ids=IDS)
def test_query_unions_the_two_row_families(category, script_cls, cfg):
    query = script_cls.QUERY

    assert query.count("UNION ALL") == 1
    assert "file_date::timestamp AS file_date" in query


def test_all_metrics_views_are_registered():
    ViewScriptFactory.clear()
    jobs._register()

    for category, script_cls, _cfg in CASES:
        script = ViewScriptFactory.get(f"metrics_{category}_vw")
        assert isinstance(script, script_cls)
        assert script.TARGET_VIEW in ViewScriptFactory.scripts()
