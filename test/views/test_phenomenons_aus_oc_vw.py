"""The ``phenomenons_aus_oc_vw`` script — strict M49 Australia & Oceania."""

from views.phenomenons_africa_vw import COUNTRY_CODES as AFRICA_CODES
from views.phenomenons_asia_vw import COUNTRY_CODES as ASIA_CODES
from views.phenomenons_aus_oc_vw import COUNTRY_CODES, PhenomenonsAusOcVwScript, _cfg
from views.phenomenons_europe_vw import COUNTRY_CODES as EUROPE_CODES
from views.phenomenons_us_vw import PhenomenonsUsVwScript

#: The four UN M49 Oceania sub-regions the whitelist must cover.
AUSTRALIA_NEW_ZEALAND = frozenset(["AU", "CC", "CX", "NF", "NZ"])
MELANESIA = frozenset(["FJ", "NC", "PG", "SB", "VU"])
MICRONESIA = frozenset(["FM", "GU", "KI", "MH", "MP", "NR", "PW", "UM"])
POLYNESIA = frozenset(["AS", "CK", "NU", "PF", "PN", "TK", "TO", "TV", "WF", "WS"])


def test_source_is_the_union_etl_table():
    assert PhenomenonsAusOcVwScript.SOURCE_TABLE == "union_etl"
    assert _cfg.source_table == "union_etl"


def test_target_view_comes_from_the_config_and_ends_with_vw():
    assert PhenomenonsAusOcVwScript.TARGET_VIEW == "phenomenons_aus_oc_vw"
    assert _cfg.target_view == PhenomenonsAusOcVwScript.TARGET_VIEW
    assert PhenomenonsAusOcVwScript.TARGET_VIEW.endswith("_vw")


def test_query_comes_from_the_config():
    assert _cfg.query == PhenomenonsAusOcVwScript.QUERY


def test_query_filters_by_the_aus_oc_whitelist():
    query = PhenomenonsAusOcVwScript.QUERY

    assert "FROM geo.union_etl" in query
    assert "WHERE country_code IN (" in query


def test_query_select_is_identical_to_the_us_view():
    us_prefix = PhenomenonsUsVwScript.QUERY.split(" WHERE country_code")[0]
    aus_oc_prefix = PhenomenonsAusOcVwScript.QUERY.split(" WHERE country_code")[0]

    assert aus_oc_prefix == us_prefix


def test_whitelist_matches_the_query():
    query = PhenomenonsAusOcVwScript.QUERY
    in_list = query.split("WHERE country_code IN (", 1)[1].rsplit(")", 1)[0]
    codes_in_query = tuple(part.strip().strip("'") for part in in_list.split(","))

    assert codes_in_query == COUNTRY_CODES
    assert len(COUNTRY_CODES) == 28


def test_whitelist_covers_all_four_m49_oceania_sub_regions():
    assert set(COUNTRY_CODES) >= AUSTRALIA_NEW_ZEALAND
    assert set(COUNTRY_CODES) >= MELANESIA
    assert set(COUNTRY_CODES) >= MICRONESIA
    assert set(COUNTRY_CODES) >= POLYNESIA
    assert set(COUNTRY_CODES) == (AUSTRALIA_NEW_ZEALAND | MELANESIA | MICRONESIA | POLYNESIA)


def test_whitelist_excludes_other_regions_and_unknown():
    forbidden = {"RU", "ZZ", "US", "CA", "TR", "CY", "EG", "RE", "YT", "SH", "IO", "TL", "AQ", "GS"}

    assert not forbidden & set(COUNTRY_CODES)


def test_whitelist_is_disjoint_from_the_other_regional_views():
    assert not set(COUNTRY_CODES) & set(EUROPE_CODES)
    assert not set(COUNTRY_CODES) & set(ASIA_CODES)
    assert not set(COUNTRY_CODES) & set(AFRICA_CODES)


def test_whitelist_is_ordered_and_upper_case():
    assert list(COUNTRY_CODES) == sorted(COUNTRY_CODES)
    assert all(len(code) == 2 and code.isupper() for code in COUNTRY_CODES)


def test_query_keeps_the_display_shape():
    query = PhenomenonsAusOcVwScript.QUERY

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

    script = ViewScriptFactory.get("phenomenons_aus_oc_vw")
    assert isinstance(script, PhenomenonsAusOcVwScript)
    assert script.TARGET_VIEW in ViewScriptFactory.scripts()
