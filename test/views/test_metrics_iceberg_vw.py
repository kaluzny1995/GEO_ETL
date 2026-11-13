"""The ``metrics_iceberg_vw`` script — observed + predicted rows."""

from views.metrics_iceberg_vw import MetricsIcebergVwScript, _cfg


def test_source_is_the_shared_observed_view():
    assert MetricsIcebergVwScript.SOURCE_TABLE == "metrics_observed_vw"
    assert _cfg.source_table == "metrics_observed_vw"


def test_target_view_comes_from_the_config_and_ends_with_vw():
    assert MetricsIcebergVwScript.TARGET_VIEW == "metrics_iceberg_vw"
    assert _cfg.target_view == MetricsIcebergVwScript.TARGET_VIEW
    assert MetricsIcebergVwScript.TARGET_VIEW.endswith("_vw")


def test_query_comes_from_the_config():
    assert _cfg.query == MetricsIcebergVwScript.QUERY


def test_query_reads_observed_and_predicted_rows():
    query = MetricsIcebergVwScript.QUERY

    assert "FROM geo.metrics_observed_vw" in query
    assert "FROM geo.prediction_etl" in query
    assert "predicted_value AS metric_value" in query
    assert "is_predicted" in query
    assert query.count("WHERE event_category = 'iceberg'") == 2
    for column in ("event_category", "event_subcategory", "metric_name", "metric_value", "metric_unit", "file_date"):
        assert column in query


def test_query_unions_the_two_row_families():
    query = MetricsIcebergVwScript.QUERY

    assert query.count("UNION ALL") == 1
    assert "file_date::timestamp AS file_date" in query


def test_registered_in_the_view_script_factory():
    import jobs
    from factory import ViewScriptFactory

    ViewScriptFactory.clear()
    jobs._register()

    script = ViewScriptFactory.get("metrics_iceberg_vw")
    assert isinstance(script, MetricsIcebergVwScript)
    assert script.TARGET_VIEW in ViewScriptFactory.scripts()
