"""The ``phenomenons_europe_vw`` script — strict M49 Europe, no Russia."""

from views.phenomenons_europe_vw import COUNTRY_CODES


def test_whitelist_excludes_russia_and_the_m49_western_asia_edge_cases():
    forbidden = {"RU", "TR", "CY", "AM", "AZ", "GE", "ZZ", "US", "CA"}

    assert not forbidden & set(COUNTRY_CODES)
