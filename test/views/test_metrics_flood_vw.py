"""The ``metrics_flood_vw`` script — the flood twin of metrics_avalanche_vw."""

from views.metrics_flood_vw import MetricsFloodVwScript, _cfg


def test_source_is_the_union_etl_table():
    assert MetricsFloodVwScript.SOURCE_TABLE == "union_etl"
    assert _cfg.source_table == "union_etl"


def test_target_view_comes_from_the_config_and_ends_with_vw():
    assert MetricsFloodVwScript.TARGET_VIEW == "metrics_flood_vw"
    assert _cfg.target_view == MetricsFloodVwScript.TARGET_VIEW
    assert MetricsFloodVwScript.TARGET_VIEW.endswith("_vw")


def test_query_comes_from_the_config():
    assert _cfg.query == MetricsFloodVwScript.QUERY


def test_query_reads_the_requested_columns_of_the_flood_rows():
    query = MetricsFloodVwScript.QUERY

    assert "FROM geo.union_etl" in query
    assert "event_category = 'flood'" in query
    for column in ("event_category", "event_subcategory", "metric_name", "metric_value", "metric_unit"):
        assert column in query


def test_query_appends_the_numeric_other_metrics_entries():
    query = MetricsFloodVwScript.QUERY

    assert "other_metrics" in query
    assert "jsonb_each" in query
    assert "jsonb_typeof(e.value) = 'number'" in query  # non-numeric entries carry no measurement
    assert "e.key || '_unit'" in query  # unit from the companion <name>_unit key
    assert "NOT LIKE '%\\_unit'" in query  # companion keys are units, not metrics
    assert query.count("UNION ALL") == 1  # own metric rows + other-metric rows


def test_query_skips_own_metrics_without_a_value():
    query = MetricsFloodVwScript.QUERY

    # bare names with no measurement are dropped — only measured rows remain
    assert "WHERE metric_value IS NOT NULL" in query


def test_query_casts_file_date_to_timestamp_at_the_end():
    query = MetricsFloodVwScript.QUERY

    assert "file_date::timestamp AS file_date" in query  # YYYY-MM-DD stamp string -> datetime
    assert "a.file_date" in query  # carried through the other_metrics branch too
    assert "metric_unit, file_date" in query  # trailing column of both final SELECTs


def test_query_limits_metrics_to_the_whitelist():
    query = MetricsFloodVwScript.QUERY

    whitelist = "metric_name IN ('discharge', 'threshold', 'baseline_days', 'exceedance_ratio', 'magnitude')"
    assert query.count(whitelist) == 2  # both final SELECTs


def test_registered_in_the_view_script_factory():
    import jobs
    from factory import ViewScriptFactory

    # Earlier tests clear the registry — re-run the import-time registration.
    ViewScriptFactory.clear()
    jobs._register()

    script = ViewScriptFactory.get("metrics_flood_vw")
    assert isinstance(script, MetricsFloodVwScript)
    assert script.TARGET_VIEW in ViewScriptFactory.scripts()
