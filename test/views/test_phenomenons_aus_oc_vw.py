"""The ``phenomenons_aus_oc_vw`` script — strict M49 Australia & Oceania."""

from views.phenomenons_africa_vw import COUNTRY_CODES as AFRICA_CODES
from views.phenomenons_asia_vw import COUNTRY_CODES as ASIA_CODES
from views.phenomenons_aus_oc_vw import COUNTRY_CODES
from views.phenomenons_europe_vw import COUNTRY_CODES as EUROPE_CODES

#: The four UN M49 Oceania sub-regions the whitelist must cover.
AUSTRALIA_NEW_ZEALAND = frozenset(["AU", "CC", "CX", "HM", "NF", "NZ"])
MELANESIA = frozenset(["FJ", "NC", "PG", "SB", "VU"])
MICRONESIA = frozenset(["FM", "GU", "KI", "MH", "MP", "NR", "PW", "UM"])
POLYNESIA = frozenset(["AS", "CK", "NU", "PF", "PN", "TK", "TO", "TV", "WF", "WS"])


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
