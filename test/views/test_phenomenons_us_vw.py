"""The ``phenomenons_us_vw`` script — the US map view for Looker Studio."""

from views.phenomenons_us_vw import PhenomenonsUsVwScript, _cfg


def test_source_is_the_union_etl_table():
    assert PhenomenonsUsVwScript.SOURCE_TABLE == "union_etl"
    assert _cfg.source_table == "union_etl"


def test_target_view_comes_from_the_config_and_ends_with_vw():
    assert PhenomenonsUsVwScript.TARGET_VIEW == "phenomenons_us_vw"
    assert _cfg.target_view == PhenomenonsUsVwScript.TARGET_VIEW
    assert PhenomenonsUsVwScript.TARGET_VIEW.endswith("_vw")


def test_query_comes_from_the_config():
    assert _cfg.query == PhenomenonsUsVwScript.QUERY


def test_query_is_limited_to_united_states_rows():
    query = PhenomenonsUsVwScript.QUERY

    assert "FROM geo.union_etl" in query
    assert "WHERE country_code = 'US'" in query


def test_query_capitalizes_the_category_columns():
    query = PhenomenonsUsVwScript.QUERY

    assert "initcap(event_category) AS event_category" in query
    assert "initcap(event_subcategory) AS event_subcategory" in query


def test_query_builds_the_display_name_with_fallback_and_description():
    query = PhenomenonsUsVwScript.QUERY

    # name falls back to the capitalized category; description joins with ":\n\n"
    assert "COALESCE(event_name, initcap(event_category))" in query
    assert "E':\\n\\n' || event_description" in query
    assert "AS event_ndesc" in query


def test_query_passes_danger_rating_and_formats_other_metrics():
    query = PhenomenonsUsVwScript.QUERY

    # categorical danger_rating column, in the danger_level slot (after place_location)
    assert "place_location, danger_rating," in query
    assert "danger_level" not in query
    # Looker-friendly "key: value (unit); ..." text, companions excluded
    assert "string_agg" in query
    assert "AS other_metrics" in query
    assert "NOT LIKE '%\\_unit'" in query


def test_query_coalesces_the_start_date_and_keeps_the_end_date():
    query = PhenomenonsUsVwScript.QUERY

    assert "COALESCE(event_start, file_date::timestamp) AS event_start_date" in query
    assert "event_end AS event_end_date" in query


def test_query_lists_the_map_columns():
    query = PhenomenonsUsVwScript.QUERY

    for column in (
        "latitude",
        "longitude",
        "country_code",
        "state_province",
        "place_location",
        "metric_name",
        "metric_value",
        "metric_unit",
    ):
        assert column in query


def test_registered_in_the_view_script_factory():
    import jobs
    from factory import ViewScriptFactory

    # Earlier tests clear the registry — re-run the import-time registration.
    ViewScriptFactory.clear()
    jobs._register()

    script = ViewScriptFactory.get("phenomenons_us_vw")
    assert isinstance(script, PhenomenonsUsVwScript)
    assert script.TARGET_VIEW in ViewScriptFactory.scripts()
