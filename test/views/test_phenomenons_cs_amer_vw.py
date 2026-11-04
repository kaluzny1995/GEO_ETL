"""The ``phenomenons_cs_amer_vw`` script — the Americas twin (minus US/CA)."""

from views.phenomenons_cs_amer_vw import COUNTRY_CODES


def test_whitelist_covers_the_five_m49_codes_missed_by_the_first_audit():
    # BM/KY had no records yet; SV (8 rows) and GS (3 rows) were uncovered.
    assert set(COUNTRY_CODES) >= {"BM", "BV", "GS", "KY", "SV"}


def test_whitelist_excludes_us_canada_and_unknown():
    assert not {"US", "CA", "ZZ"} & set(COUNTRY_CODES)
