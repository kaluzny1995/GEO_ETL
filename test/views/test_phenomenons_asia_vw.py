"""The ``phenomenons_asia_vw`` script — strict M49 Asia, no Russia, Europe's complement."""

from views.phenomenons_asia_vw import COUNTRY_CODES
from views.phenomenons_europe_vw import COUNTRY_CODES as EUROPE_CODES


def test_whitelist_excludes_russia_unknown_and_other_regions():
    forbidden = {"RU", "ZZ", "US", "CA", "AU", "NZ", "PG"}

    assert not forbidden & set(COUNTRY_CODES)


def test_whitelist_holds_the_m49_western_asia_edge_cases():
    # Complementary to the Europe view: these are M49 "Western Asia".
    assert {"TR", "CY", "AM", "AZ", "GE"} <= set(COUNTRY_CODES)


def test_whitelist_is_disjoint_from_the_europe_view():
    assert not set(COUNTRY_CODES) & set(EUROPE_CODES)
