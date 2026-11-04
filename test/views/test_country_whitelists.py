"""Parametrized contracts shared by the regional ``phenomenons_*_vw`` views."""

from dataclasses import dataclass

import pytest

from views import (
    phenomenons_africa_vw,
    phenomenons_asia_vw,
    phenomenons_aus_oc_vw,
    phenomenons_cs_amer_vw,
    phenomenons_europe_vw,
)
from views.phenomenons_us_vw import PhenomenonsUsVwScript


@dataclass(frozen=True)
class Case:
    """One regional view and the whitelist it must expose."""

    script_cls: type
    cfg: object
    country_codes: tuple[str, ...]
    expected_len: int


CASES = [
    pytest.param(
        Case(
            phenomenons_cs_amer_vw.PhenomenonsCsAmerVwScript,
            phenomenons_cs_amer_vw._cfg,
            phenomenons_cs_amer_vw.COUNTRY_CODES,
            55,
        ),
        id="cs_amer",
    ),
    pytest.param(
        Case(
            phenomenons_europe_vw.PhenomenonsEuropeVwScript,
            phenomenons_europe_vw._cfg,
            phenomenons_europe_vw.COUNTRY_CODES,
            51,
        ),
        id="europe",
    ),
    pytest.param(
        Case(
            phenomenons_asia_vw.PhenomenonsAsiaVwScript,
            phenomenons_asia_vw._cfg,
            phenomenons_asia_vw.COUNTRY_CODES,
            51,
        ),
        id="asia",
    ),
    pytest.param(
        Case(
            phenomenons_africa_vw.PhenomenonsAfricaVwScript,
            phenomenons_africa_vw._cfg,
            phenomenons_africa_vw.COUNTRY_CODES,
            60,
        ),
        id="africa",
    ),
    pytest.param(
        Case(
            phenomenons_aus_oc_vw.PhenomenonsAusOcVwScript,
            phenomenons_aus_oc_vw._cfg,
            phenomenons_aus_oc_vw.COUNTRY_CODES,
            29,
        ),
        id="aus_oc",
    ),
]


@pytest.mark.parametrize("case", CASES)
class TestRegionalViewContract:
    def test_source_is_the_union_etl_table(self, case):
        assert case.script_cls.SOURCE_TABLE == "union_etl"
        assert case.cfg.source_table == "union_etl"

    def test_target_view_comes_from_the_config_and_ends_with_vw(self, case):
        assert case.cfg.target_view == case.script_cls.TARGET_VIEW
        assert case.script_cls.TARGET_VIEW.endswith("_vw")

    def test_query_comes_from_the_config(self, case):
        assert case.cfg.query == case.script_cls.QUERY

    def test_country_codes_come_from_the_config(self, case):
        assert case.country_codes == tuple(case.cfg.country_codes)
        assert len(case.country_codes) == case.expected_len

    def test_query_filters_by_the_whitelist(self, case):
        query = case.script_cls.QUERY

        assert "FROM geo.union_etl" in query
        assert "WHERE country_code IN (" in query

    def test_whitelist_matches_the_query(self, case):
        query = case.script_cls.QUERY
        in_list = query.split("WHERE country_code IN (", 1)[1].rsplit(")", 1)[0]
        codes_in_query = tuple(part.strip().strip("'") for part in in_list.split(","))

        assert codes_in_query == case.country_codes

    def test_whitelist_is_ordered_unique_and_upper_case(self, case):
        assert list(case.country_codes) == sorted(case.country_codes)
        assert len(set(case.country_codes)) == len(case.country_codes)
        assert all(len(code) == 2 and code.isupper() for code in case.country_codes)

    def test_query_select_is_identical_to_the_us_view(self, case):
        us_prefix = PhenomenonsUsVwScript.QUERY.split(" WHERE country_code")[0]
        prefix = case.script_cls.QUERY.split(" WHERE country_code")[0]

        assert prefix == us_prefix

    def test_query_keeps_the_display_shape(self, case):
        query = case.script_cls.QUERY

        assert "initcap(event_category) AS event_category" in query
        assert "COALESCE(event_name, initcap(event_category))" in query
        assert "E':\\n\\n' || event_description" in query
        assert "AS event_ndesc" in query
        assert "place_location, danger_rating," in query
        assert "danger_level" not in query
        assert "NOT LIKE '%\\_unit'" in query
        assert "COALESCE(event_start, file_date::timestamp) AS event_start_date" in query
        assert "event_end AS event_end_date" in query

    def test_registered_in_the_view_script_factory(self, case):
        import jobs
        from factory import ViewScriptFactory

        # Earlier tests clear the registry — re-run the import-time registration.
        ViewScriptFactory.clear()
        jobs._register()

        script = ViewScriptFactory.get(case.script_cls.TARGET_VIEW)
        assert isinstance(script, case.script_cls)
        assert script.TARGET_VIEW in ViewScriptFactory.scripts()
