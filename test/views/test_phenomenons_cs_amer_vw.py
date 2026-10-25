"""The ``phenomenons_cs_amer_vw`` script — the Americas twin (minus US/CA)."""

from views.phenomenons_cs_amer_vw import COUNTRY_CODES, PhenomenonsCsAmerVwScript, _cfg
from views.phenomenons_us_vw import PhenomenonsUsVwScript


def test_source_is_the_union_etl_table():
    assert PhenomenonsCsAmerVwScript.SOURCE_TABLE == "union_etl"
    assert _cfg.source_table == "union_etl"


def test_target_view_comes_from_the_config_and_ends_with_vw():
    assert PhenomenonsCsAmerVwScript.TARGET_VIEW == "phenomenons_cs_amer_vw"
    assert _cfg.target_view == PhenomenonsCsAmerVwScript.TARGET_VIEW
    assert PhenomenonsCsAmerVwScript.TARGET_VIEW.endswith("_vw")


def test_query_comes_from_the_config():
    assert _cfg.query == PhenomenonsCsAmerVwScript.QUERY


def test_query_filters_by_the_americas_whitelist():
    query = PhenomenonsCsAmerVwScript.QUERY

    assert "FROM geo.union_etl" in query
    assert "WHERE country_code IN (" in query


def test_query_select_is_identical_to_the_us_view():
    us_prefix = PhenomenonsUsVwScript.QUERY.split(" WHERE country_code")[0]
    cs_amer_prefix = PhenomenonsCsAmerVwScript.QUERY.split(" WHERE country_code")[0]

    assert cs_amer_prefix == us_prefix


def test_whitelist_matches_the_query_and_excludes_us_canada_and_unknown():
    query = PhenomenonsCsAmerVwScript.QUERY
    in_list = query.split("WHERE country_code IN (", 1)[1].rsplit(")", 1)[0]
    codes_in_query = tuple(part.strip().strip("'") for part in in_list.split(","))

    assert codes_in_query == COUNTRY_CODES
    assert not {"US", "CA", "ZZ"} & set(COUNTRY_CODES)
    assert len(COUNTRY_CODES) == 55


def test_whitelist_covers_the_five_m49_codes_missed_by_the_first_audit():
    # BM/KY had no records yet; SV (8 rows) and GS (3 rows) were uncovered.
    assert set(COUNTRY_CODES) >= {"BM", "BV", "GS", "KY", "SV"}


def test_whitelist_is_ordered_and_upper_case():
    assert list(COUNTRY_CODES) == sorted(COUNTRY_CODES)
    assert all(len(code) == 2 and code.isupper() for code in COUNTRY_CODES)


def test_query_keeps_the_display_shape():
    query = PhenomenonsCsAmerVwScript.QUERY

    assert "initcap(event_category) AS event_category" in query
    assert "COALESCE(event_name, initcap(event_category))" in query
    assert "E':\\n\\n' || event_description" in query
    assert "AS event_ndesc" in query
    assert "place_location, danger_rating," in query
    assert "danger_level" not in query
    assert "NOT LIKE '%\\_unit'" in query
    assert "COALESCE(event_start, file_date::timestamp) AS event_start_date" in query
    assert "event_end AS event_end_date" in query


def test_registered_in_the_view_script_factory():
    import jobs
    from factory import ViewScriptFactory

    # Earlier tests clear the registry — re-run the import-time registration.
    ViewScriptFactory.clear()
    jobs._register()

    script = ViewScriptFactory.get("phenomenons_cs_amer_vw")
    assert isinstance(script, PhenomenonsCsAmerVwScript)
    assert script.TARGET_VIEW in ViewScriptFactory.scripts()
