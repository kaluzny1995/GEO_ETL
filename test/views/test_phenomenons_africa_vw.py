"""The ``phenomenons_africa_vw`` script — strict M49 Africa (all 54 AU members + territories)."""

from views.phenomenons_africa_vw import COUNTRY_CODES
from views.phenomenons_asia_vw import COUNTRY_CODES as ASIA_CODES
from views.phenomenons_europe_vw import COUNTRY_CODES as EUROPE_CODES

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


def test_whitelist_covers_all_54_au_member_states():
    assert set(COUNTRY_CODES) >= AU_54


def test_whitelist_excludes_other_regions_and_unknown():
    forbidden = {"RU", "TR", "CY", "AM", "AZ", "GE", "ZZ", "US", "CA", "AU", "NZ"}

    assert not forbidden & set(COUNTRY_CODES)


def test_whitelist_is_disjoint_from_the_europe_and_asia_views():
    assert not set(COUNTRY_CODES) & set(EUROPE_CODES)
    assert not set(COUNTRY_CODES) & set(ASIA_CODES)
