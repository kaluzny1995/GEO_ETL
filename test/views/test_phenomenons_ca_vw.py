"""The ``phenomenons_ca_vw`` script — the Canada twin of phenomenons_us_vw."""

from views.phenomenons_ca_vw import PhenomenonsCaVwScript, _cfg
from views.phenomenons_us_vw import PhenomenonsUsVwScript


def test_source_is_the_union_etl_table():
    assert PhenomenonsCaVwScript.SOURCE_TABLE == "union_etl"
    assert _cfg.source_table == "union_etl"


def test_target_view_comes_from_the_config_and_ends_with_vw():
    assert PhenomenonsCaVwScript.TARGET_VIEW == "phenomenons_ca_vw"
    assert _cfg.target_view == PhenomenonsCaVwScript.TARGET_VIEW
    assert PhenomenonsCaVwScript.TARGET_VIEW.endswith("_vw")


def test_query_comes_from_the_config():
    assert _cfg.query == PhenomenonsCaVwScript.QUERY


def test_query_is_limited_to_canada_rows():
    query = PhenomenonsCaVwScript.QUERY

    assert "FROM geo.union_etl" in query
    assert "WHERE country_code = 'CA'" in query


def test_query_differs_from_the_us_view_only_in_the_country_code():
    swapped = PhenomenonsUsVwScript.QUERY.replace("country_code = 'US'", "country_code = 'CA'")

    assert swapped == PhenomenonsCaVwScript.QUERY


def test_query_keeps_the_us_view_display_shape():
    query = PhenomenonsCaVwScript.QUERY

    assert "initcap(event_category) AS event_category" in query
    assert "initcap(event_subcategory) AS event_subcategory" in query
    assert "COALESCE(event_name, initcap(event_category))" in query
    assert "E':\\n\\n' || event_description" in query
    assert "AS event_ndesc" in query
    assert "place_location, danger_rating," in query
    assert "danger_level" not in query
    assert "string_agg" in query
    assert "NOT LIKE '%\\_unit'" in query
    assert "COALESCE(event_start, file_date::timestamp) AS event_start_date" in query
    assert "event_end AS event_end_date" in query


def test_query_lists_the_map_columns():
    query = PhenomenonsCaVwScript.QUERY

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

    script = ViewScriptFactory.get("phenomenons_ca_vw")
    assert isinstance(script, PhenomenonsCaVwScript)
    assert script.TARGET_VIEW in ViewScriptFactory.scripts()
