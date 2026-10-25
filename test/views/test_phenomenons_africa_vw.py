"""The ``phenomenons_africa_vw`` script — strict M49 Africa (all 54 AU members + territories)."""

from views.phenomenons_africa_vw import COUNTRY_CODES, PhenomenonsAfricaVwScript, _cfg
from views.phenomenons_asia_vw import COUNTRY_CODES as ASIA_CODES
from views.phenomenons_europe_vw import COUNTRY_CODES as EUROPE_CODES
from views.phenomenons_us_vw import PhenomenonsUsVwScript

#: The 54 African Union member states — every one must be in the whitelist.
AU_54 = frozenset(
    [
        "DZ",
        "AO",
        "BJ",
        "BW",
        "BF",
        "BI",
        "CM",
        "CV",
        "CF",
        "TD",
        "KM",
        "CG",
        "CD",
        "CI",
        "DJ",
        "EG",
        "GQ",
        "ER",
        "SZ",
        "ET",
        "GA",
        "GM",
        "GH",
        "GN",
        "GW",
        "KE",
        "LS",
        "LR",
        "LY",
        "MG",
        "MW",
        "ML",
        "MR",
        "MU",
        "MA",
        "MZ",
        "NA",
        "NE",
        "NG",
        "RW",
        "ST",
        "SN",
        "SC",
        "SL",
        "SO",
        "ZA",
        "SS",
        "SD",
        "TZ",
        "TG",
        "TN",
        "UG",
        "ZM",
        "ZW",
    ]
)


def test_source_is_the_union_etl_table():
    assert PhenomenonsAfricaVwScript.SOURCE_TABLE == "union_etl"
    assert _cfg.source_table == "union_etl"


def test_target_view_comes_from_the_config_and_ends_with_vw():
    assert PhenomenonsAfricaVwScript.TARGET_VIEW == "phenomenons_africa_vw"
    assert _cfg.target_view == PhenomenonsAfricaVwScript.TARGET_VIEW
    assert PhenomenonsAfricaVwScript.TARGET_VIEW.endswith("_vw")


def test_query_comes_from_the_config():
    assert _cfg.query == PhenomenonsAfricaVwScript.QUERY


def test_query_filters_by_the_africa_whitelist():
    query = PhenomenonsAfricaVwScript.QUERY

    assert "FROM geo.union_etl" in query
    assert "WHERE country_code IN (" in query


def test_query_select_is_identical_to_the_us_view():
    us_prefix = PhenomenonsUsVwScript.QUERY.split(" WHERE country_code")[0]
    africa_prefix = PhenomenonsAfricaVwScript.QUERY.split(" WHERE country_code")[0]

    assert africa_prefix == us_prefix


def test_whitelist_matches_the_query():
    query = PhenomenonsAfricaVwScript.QUERY
    in_list = query.split("WHERE country_code IN (", 1)[1].rsplit(")", 1)[0]
    codes_in_query = tuple(part.strip().strip("'") for part in in_list.split(","))

    assert codes_in_query == COUNTRY_CODES
    assert len(COUNTRY_CODES) == 60


def test_whitelist_covers_all_54_au_member_states():
    assert set(COUNTRY_CODES) >= AU_54


def test_whitelist_excludes_other_regions_and_unknown():
    forbidden = {"RU", "TR", "CY", "AM", "AZ", "GE", "ZZ", "US", "CA", "AU", "NZ"}

    assert not forbidden & set(COUNTRY_CODES)


def test_whitelist_is_disjoint_from_the_europe_and_asia_views():
    assert not set(COUNTRY_CODES) & set(EUROPE_CODES)
    assert not set(COUNTRY_CODES) & set(ASIA_CODES)


def test_whitelist_is_ordered_and_upper_case():
    assert list(COUNTRY_CODES) == sorted(COUNTRY_CODES)
    assert all(len(code) == 2 and code.isupper() for code in COUNTRY_CODES)


def test_query_keeps_the_display_shape():
    query = PhenomenonsAfricaVwScript.QUERY

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

    script = ViewScriptFactory.get("phenomenons_africa_vw")
    assert isinstance(script, PhenomenonsAfricaVwScript)
    assert script.TARGET_VIEW in ViewScriptFactory.scripts()
