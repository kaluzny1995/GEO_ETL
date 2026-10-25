"""The ``phenomenons_asia_vw`` script — strict M49 Asia, no Russia, Europe's complement."""

from views.phenomenons_asia_vw import COUNTRY_CODES, PhenomenonsAsiaVwScript, _cfg
from views.phenomenons_europe_vw import COUNTRY_CODES as EUROPE_CODES
from views.phenomenons_us_vw import PhenomenonsUsVwScript


def test_source_is_the_union_etl_table():
    assert PhenomenonsAsiaVwScript.SOURCE_TABLE == "union_etl"
    assert _cfg.source_table == "union_etl"


def test_target_view_comes_from_the_config_and_ends_with_vw():
    assert PhenomenonsAsiaVwScript.TARGET_VIEW == "phenomenons_asia_vw"
    assert _cfg.target_view == PhenomenonsAsiaVwScript.TARGET_VIEW
    assert PhenomenonsAsiaVwScript.TARGET_VIEW.endswith("_vw")


def test_query_comes_from_the_config():
    assert _cfg.query == PhenomenonsAsiaVwScript.QUERY


def test_query_filters_by_the_asia_whitelist():
    query = PhenomenonsAsiaVwScript.QUERY

    assert "FROM geo.union_etl" in query
    assert "WHERE country_code IN (" in query


def test_query_select_is_identical_to_the_us_view():
    us_prefix = PhenomenonsUsVwScript.QUERY.split(" WHERE country_code")[0]
    asia_prefix = PhenomenonsAsiaVwScript.QUERY.split(" WHERE country_code")[0]

    assert asia_prefix == us_prefix


def test_whitelist_matches_the_query():
    query = PhenomenonsAsiaVwScript.QUERY
    in_list = query.split("WHERE country_code IN (", 1)[1].rsplit(")", 1)[0]
    codes_in_query = tuple(part.strip().strip("'") for part in in_list.split(","))

    assert codes_in_query == COUNTRY_CODES
    assert len(COUNTRY_CODES) == 51


def test_whitelist_excludes_russia_unknown_and_other_regions():
    forbidden = {"RU", "ZZ", "US", "CA", "AU", "NZ", "PG"}

    assert not forbidden & set(COUNTRY_CODES)


def test_whitelist_holds_the_m49_western_asia_edge_cases():
    # Complementary to the Europe view: these are M49 "Western Asia".
    assert {"TR", "CY", "AM", "AZ", "GE"} <= set(COUNTRY_CODES)


def test_whitelist_is_disjoint_from_the_europe_view():
    assert not set(COUNTRY_CODES) & set(EUROPE_CODES)


def test_whitelist_is_ordered_and_upper_case():
    assert list(COUNTRY_CODES) == sorted(COUNTRY_CODES)
    assert all(len(code) == 2 and code.isupper() for code in COUNTRY_CODES)


def test_query_keeps_the_display_shape():
    query = PhenomenonsAsiaVwScript.QUERY

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

    script = ViewScriptFactory.get("phenomenons_asia_vw")
    assert isinstance(script, PhenomenonsAsiaVwScript)
    assert script.TARGET_VIEW in ViewScriptFactory.scripts()
