"""The shared ``metrics_observed_vw`` base view over ``geo.union_etl``."""

from views.metrics_observed_vw import MetricsObservedVwScript, _cfg


def test_source_is_the_union_etl_table():
    assert MetricsObservedVwScript.SOURCE_TABLE == "union_etl"
    assert _cfg.source_table == "union_etl"


def test_target_view_comes_from_the_config_and_ends_with_vw():
    assert MetricsObservedVwScript.TARGET_VIEW == "metrics_observed_vw"
    assert _cfg.target_view == MetricsObservedVwScript.TARGET_VIEW
    assert MetricsObservedVwScript.TARGET_VIEW.endswith("_vw")


def test_query_covers_every_phenomenon_category():
    query = MetricsObservedVwScript.QUERY

    for category in ("avalanche", "drought", "earthquake", "fire", "flood", "iceberg", "storm", "volcano"):
        assert f"'{category}'" in query
    assert "FROM geo.union_etl" in query
    assert "event_category" in query


def test_query_explodes_numeric_other_metrics():
    query = MetricsObservedVwScript.QUERY

    assert "jsonb_each" in query
    assert "jsonb_typeof(e.value) = 'number'" in query
    assert "e.key || '_unit'" in query
    assert "NOT LIKE '%\\_unit'" in query
    assert query.count("UNION ALL") == 1


def test_query_casts_file_date_and_marks_observed_rows():
    query = MetricsObservedVwScript.QUERY

    assert "file_date::timestamp AS file_date" in query
    assert query.count("FALSE AS is_predicted") == 2


def test_query_keeps_the_earthquake_and_flood_filters():
    query = MetricsObservedVwScript.QUERY

    assert "metric_unit NOT IN ('mh', 'Mi', 'mun', 'ms_vx')" in query
    assert query.count("metric_unit NOT IN") == 2
    whitelist = "metric_name IN ('discharge', 'threshold', 'baseline_days', 'exceedance_ratio', 'magnitude')"
    assert query.count(whitelist) == 2


def test_registered_in_the_view_script_factory():
    import jobs
    from factory import ViewScriptFactory

    ViewScriptFactory.clear()
    jobs._register()

    script = ViewScriptFactory.get("metrics_observed_vw")
    assert isinstance(script, MetricsObservedVwScript)
    assert script.TARGET_VIEW in ViewScriptFactory.scripts()
