"""The ``metrics_avalanche_vw`` script — observed + predicted rows."""

from views.metrics_avalanche_vw import MetricsAvalancheVwScript, _cfg


def test_source_is_the_shared_observed_view():
    assert MetricsAvalancheVwScript.SOURCE_TABLE == "metrics_observed_vw"
    assert _cfg.source_table == "metrics_observed_vw"


def test_target_view_comes_from_the_config_and_ends_with_vw():
    assert MetricsAvalancheVwScript.TARGET_VIEW == "metrics_avalanche_vw"
    assert _cfg.target_view == MetricsAvalancheVwScript.TARGET_VIEW
    assert MetricsAvalancheVwScript.TARGET_VIEW.endswith("_vw")


def test_query_comes_from_the_config():
    assert _cfg.query == MetricsAvalancheVwScript.QUERY


def test_query_reads_observed_and_predicted_rows():
    query = MetricsAvalancheVwScript.QUERY

    assert "FROM geo.metrics_observed_vw" in query
    assert "FROM geo.prediction_etl" in query
    assert "predicted_value AS metric_value" in query
    assert "is_predicted" in query
    assert query.count("WHERE event_category = 'avalanche'") == 2
    for column in ("event_category", "event_subcategory", "metric_name", "metric_value", "metric_unit", "file_date"):
        assert column in query


def test_query_unions_the_two_row_families():
    query = MetricsAvalancheVwScript.QUERY

    assert query.count("UNION ALL") == 1
    assert "file_date::timestamp AS file_date" in query


def test_registered_in_the_view_script_factory():
    import jobs
    from factory import ViewScriptFactory

    ViewScriptFactory.clear()
    jobs._register()

    script = ViewScriptFactory.get("metrics_avalanche_vw")
    assert isinstance(script, MetricsAvalancheVwScript)
    assert script.TARGET_VIEW in ViewScriptFactory.scripts()
