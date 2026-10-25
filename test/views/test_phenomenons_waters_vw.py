"""The ``phenomenons_waters_vw`` script — open-water (ocean/sea) events."""

from views.phenomenons_us_vw import PhenomenonsUsVwScript
from views.phenomenons_waters_vw import PhenomenonsWatersVwScript, _cfg

KEYWORDS = ("ocean", "sea", "gulf", "strait", "offshore", "coast")


def test_source_is_the_union_etl_table():
    assert PhenomenonsWatersVwScript.SOURCE_TABLE == "union_etl"
    assert _cfg.source_table == "union_etl"


def test_target_view_comes_from_the_config_and_ends_with_vw():
    assert PhenomenonsWatersVwScript.TARGET_VIEW == "phenomenons_waters_vw"
    assert _cfg.target_view == PhenomenonsWatersVwScript.TARGET_VIEW
    assert PhenomenonsWatersVwScript.TARGET_VIEW.endswith("_vw")


def test_query_comes_from_the_config():
    assert _cfg.query == PhenomenonsWatersVwScript.QUERY


def test_query_reads_union_etl_and_guards_the_geocode():
    query = PhenomenonsWatersVwScript.QUERY

    assert "FROM geo.union_etl" in query
    assert "WHERE latitude IS NOT NULL AND longitude IS NOT NULL" in query


def test_query_select_is_identical_to_the_us_view():
    us_select = PhenomenonsUsVwScript.QUERY.split(" WHERE ", 1)[0]
    waters_select = PhenomenonsWatersVwScript.QUERY.split(" WHERE ", 1)[0]

    assert waters_select == us_select


def test_query_flags_open_water_by_country_code_zz():
    query = PhenomenonsWatersVwScript.QUERY

    assert "country_code = 'ZZ'" in query
    assert "country_code IN (" not in query


def test_query_flags_iceberg_rows_by_subcategory():
    assert "event_subcategory = 'sea_lake_ice'" in PhenomenonsWatersVwScript.QUERY


def test_query_matches_maritime_keywords_case_insensitively_with_word_boundaries():
    query = PhenomenonsWatersVwScript.QUERY

    assert " ~* " in query
    for keyword in KEYWORDS:
        assert keyword in query
    assert "off the coast" in query
    assert "open water" in query
    # PostgreSQL word-boundary escape, JSON-decoded from \\y in config.json.
    assert r"\y(ocean|sea|gulf|strait|offshore|coast)\y" in query


def test_query_excludes_land_confusing_keywords():
    # Audited false positives: 'Pedro Bay'/'Cruz Bay' towns, Alaska's 'Maritime' region.
    query = PhenomenonsWatersVwScript.QUERY

    assert "bay" not in query
    assert "maritime" not in query


def test_query_keeps_the_display_shape():
    query = PhenomenonsWatersVwScript.QUERY

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

    script = ViewScriptFactory.get("phenomenons_waters_vw")
    assert isinstance(script, PhenomenonsWatersVwScript)
    assert script.TARGET_VIEW in ViewScriptFactory.scripts()
