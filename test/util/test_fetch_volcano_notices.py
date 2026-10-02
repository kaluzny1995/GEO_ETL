from unittest.mock import patch

import pytest

from utils.fetch_volcano_notices import load_notices, notice_key, notice_map, parse_notice, save_notices

NOTICE_ID = "DOI-USGS-AVO-2026-09-29T18:26:19+00:00"
PAYLOAD = {
    "notice_identifier": NOTICE_ID,
    "notice_type": "Daily Update",
    "notice_sections": [
        {
            "vnum": "311120",
            "vName": "Great Sitkin",
            "lat": 52.0765,
            "lng": -176.1109,
            "region": "Aleutians",
            "synopsis": "AVO Great Sitkin ORANGE/WATCH - Slow eruption of lava continues.",
        },
        {
            "vnum": 311360,
            "vName": "Shishaldin",
            "lat": 54.7554,
            "lng": -163.9711,
            "region": "Aleutians",
            "synopsis": "AVO Shishaldin YELLOW/ADVISORY - Low-level unrest continues.",
        },
        {"vnum": None, "lat": None, "lng": None, "region": None, "synopsis": None},
    ],
}


class TestNoticeKey:
    def test_joins_identifier_and_vnum(self):
        assert notice_key(NOTICE_ID, "311120") == f"{NOTICE_ID}|311120"

    def test_int_vnum_matches_string_vnum(self):
        assert notice_key(NOTICE_ID, 311120) == notice_key(NOTICE_ID, "311120")


class TestParseNotice:
    def test_extracts_the_matching_section(self):
        fields = parse_notice(PAYLOAD, "311120")

        assert fields == {
            "notice_type": "Daily Update",
            "latitude": "52.0765",
            "longitude": "-176.1109",
            "region": "Aleutians",
            "synopsis": "AVO Great Sitkin ORANGE/WATCH - Slow eruption of lava continues.",
        }

    def test_int_vnum_matches_string_section(self):
        assert parse_notice(PAYLOAD, 311360)["latitude"] == "54.7554"

    def test_null_section_fields_become_empty_strings(self):
        payload = {
            "notice_identifier": NOTICE_ID,
            "notice_type": None,
            "notice_sections": [{"vnum": "284141", "lat": None, "lng": None, "region": None, "synopsis": None}],
        }

        fields = parse_notice(payload, "284141")

        assert fields == {"notice_type": "", "latitude": "", "longitude": "", "region": "", "synopsis": ""}

    def test_missing_section_raises(self):
        with pytest.raises(RuntimeError, match="no section for vnum 999999"):
            parse_notice(PAYLOAD, "999999")


class TestCacheRoundtrip:
    def test_save_then_load(self, tmp_path):
        path = str(tmp_path / "notices.csv")
        mapping = {
            notice_key(NOTICE_ID, "311120"): {
                "notice_type": "Daily Update",
                "latitude": "52.0765",
                "longitude": "-176.1109",
                "region": "Aleutians",
                "synopsis": "AVO Great Sitkin ORANGE/WATCH - Slow eruption of lava continues.",
            }
        }

        assert save_notices(mapping, path) == 1
        assert load_notices(path) == mapping

    def test_missing_file_is_empty(self, tmp_path):
        assert load_notices(str(tmp_path / "nope.csv")) == {}


class TestNoticeMap:
    def test_fully_cached_keys_never_fetch(self, tmp_path):
        path = str(tmp_path / "notices.csv")
        save_notices(
            {
                notice_key(NOTICE_ID, "311120"): {
                    "notice_type": "Daily Update",
                    "latitude": "1",
                    "longitude": "2",
                    "region": "Aleutians",
                    "synopsis": "line",
                }
            },
            path,
        )

        with patch("utils.fetch_volcano_notices.fetch_notice") as mocked:
            result = notice_map([(NOTICE_ID, "311120")], output=path)

        mocked.assert_not_called()
        assert result[notice_key(NOTICE_ID, "311120")]["latitude"] == "1"

    def test_fetches_each_notice_once_for_many_volcanoes(self, tmp_path):
        path = str(tmp_path / "notices.csv")

        with patch("utils.fetch_volcano_notices.fetch_notice", return_value=PAYLOAD) as mocked:
            result = notice_map([(NOTICE_ID, "311120"), (NOTICE_ID, "311360")], output=path)

        assert mocked.call_count == 1
        mocked.assert_called_once_with(NOTICE_ID, "https://volcanoes.usgs.gov/hans-public/api/notice/getNotice")
        assert result[notice_key(NOTICE_ID, "311120")]["latitude"] == "52.0765"
        assert result[notice_key(NOTICE_ID, "311360")]["latitude"] == "54.7554"
        assert set(load_notices(path)) == {notice_key(NOTICE_ID, "311120"), notice_key(NOTICE_ID, "311360")}

    def test_failed_fetch_raises_but_successes_are_cached(self, tmp_path):
        path = str(tmp_path / "notices.csv")
        bad = "DOI-USGS-NMI-2026-09-24T20:22:11+00:00"

        def _fetch(identifier, url=None):
            if "AVO" in identifier:
                return PAYLOAD
            raise OSError("no network")

        with (
            patch("utils.fetch_volcano_notices.fetch_notice", side_effect=_fetch),
            pytest.raises(RuntimeError, match="volcano notice fetch failed for 1"),
        ):
            notice_map([(NOTICE_ID, "311120"), (bad, "284141")], output=path)

        assert notice_key(NOTICE_ID, "311120") in load_notices(path)
        assert notice_key(bad, "284141") not in load_notices(path)

    def test_bad_payload_counts_as_a_failure(self, tmp_path):
        path = str(tmp_path / "notices.csv")

        with (
            patch("utils.fetch_volcano_notices.fetch_notice", return_value=PAYLOAD),
            pytest.raises(RuntimeError, match="no section for vnum"),
        ):
            notice_map([(NOTICE_ID, "999999")], output=path)

        assert load_notices(path) == {}
