import json
from io import BytesIO
from unittest.mock import patch

import pytest

from utils.reverse_geocode import (
    USER_AGENT,
    coord_key,
    geocode_map,
    load_cache,
    parse_response,
    reverse_geocode,
    save_cache,
)


class TestCoordKey:
    def test_rounds_to_four_decimals(self):
        assert coord_key(45.45251, -104.46729) == "45.4525,-104.4673"

    def test_accepts_the_strings_stored_in_the_cache(self):
        assert coord_key("45.4525", "-104.4672") == "45.4525,-104.4672"

    def test_points_inside_the_precision_share_a_key(self):
        assert coord_key(1.0, 2.0) == coord_key(1.00001, 2.00001)


class TestParseResponse:
    def test_us_coordinates(self):
        data = {"address": {"country_code": "us", "ISO3166-2-lvl4": "US-MT"}}
        assert parse_response(data) == ("US", "MT")

    def test_non_us_subdivision_keeps_its_suffix(self):
        data = {"address": {"country_code": "mx", "ISO3166-2-lvl4": "MX-GRO"}}
        assert parse_response(data) == ("MX", "GRO")

    def test_state_level_wins_over_the_district(self):
        data = {"address": {"country_code": "us", "ISO3166-2-lvl4": "US-TX", "ISO3166-2-lvl6": "US-48167"}}
        assert parse_response(data) == ("US", "TX")

    def test_falls_back_to_the_deeper_administrative_level(self):
        data = {"address": {"country_code": "de", "ISO3166-2-lvl6": "DE-03115"}}
        assert parse_response(data) == ("DE", "03115")

    def test_open_water_has_no_country(self):
        assert parse_response({"address": {}}) == ("", "")

    def test_antarctica_answers_with_its_iso_country_code(self):
        assert parse_response({"address": {"country_code": "aq"}}) == ("AQ", "")

    def test_missing_fields(self):
        assert parse_response({}) == ("", "")


class TestReverseGeocode:
    @patch("utils.reverse_geocode.urllib.request.urlopen")
    def test_queries_the_api_and_parses_the_answer(self, mock_urlopen):
        body = json.dumps({"address": {"country_code": "mx", "ISO3166-2-lvl4": "MX-GRO"}}).encode()
        mock_urlopen.return_value = BytesIO(body)

        assert reverse_geocode(17.0, -99.0) == ("MX", "GRO")

        request = mock_urlopen.call_args.args[0]
        assert "lat=17.0" in request.full_url
        assert "lon=-99.0" in request.full_url
        assert "format=jsonv2" in request.full_url
        assert USER_AGENT in request.headers.values()

    @patch("utils.reverse_geocode.urllib.request.urlopen")
    def test_custom_endpoint(self, mock_urlopen):
        mock_urlopen.return_value = BytesIO(b"{}")

        reverse_geocode(1.0, 2.0, api_url="https://example.org/reverse")

        assert mock_urlopen.call_args.args[0].full_url.startswith("https://example.org/reverse?")


class TestCacheFile:
    def test_missing_cache_is_empty(self, tmp_path):
        assert load_cache(str(tmp_path / "missing.csv")) == {}

    def test_round_trip(self, tmp_path):
        path = str(tmp_path / "reverse_geocode.csv")
        mapping = {"45.4525,-104.4672": ("US", "MT"), "15.1433,-104.7983": ("", "")}

        assert save_cache(mapping, path) == 2
        assert load_cache(path) == mapping

    def test_creates_the_cache_directory(self, tmp_path):
        path = str(tmp_path / "cache" / "reverse_geocode.csv")

        save_cache({"1.0000,2.0000": ("US", "TX")}, path)

        assert load_cache(path) == {"1.0000,2.0000": ("US", "TX")}


class TestGeocodeMap:
    @patch("utils.reverse_geocode.reverse_geocode")
    def test_all_cached_skips_the_api(self, mock_lookup, tmp_path):
        path = str(tmp_path / "cache.csv")
        save_cache({"45.4525,-104.4672": ("US", "MT")}, path)

        mapping = geocode_map([(45.45251, -104.4672)], output=path)

        assert mapping == {"45.4525,-104.4672": ("US", "MT")}
        mock_lookup.assert_not_called()

    @patch("utils.reverse_geocode.reverse_geocode")
    def test_missing_points_are_fetched_and_cached(self, mock_lookup, tmp_path):
        path = str(tmp_path / "cache.csv")
        mock_lookup.return_value = ("US", "MT")

        mapping = geocode_map([(45.45251, -104.4672), (41.5175, -117.7421)], output=path)

        assert mapping == {"45.4525,-104.4672": ("US", "MT"), "41.5175,-117.7421": ("US", "MT")}
        assert load_cache(path) == mapping
        assert mock_lookup.call_count == 2

    @patch("utils.reverse_geocode.reverse_geocode")
    def test_points_sharing_a_key_are_fetched_once(self, mock_lookup, tmp_path):
        path = str(tmp_path / "cache.csv")
        mock_lookup.return_value = ("US", "MT")

        geocode_map([(1.0, 2.0), (1.00001, 2.00001)], output=path)

        mock_lookup.assert_called_once()

    @patch("utils.reverse_geocode.reverse_geocode")
    def test_failed_fetch_raises_but_keeps_the_successes(self, mock_lookup, tmp_path):
        path = str(tmp_path / "cache.csv")
        mock_lookup.side_effect = [("US", "MT"), OSError("no network")]

        with pytest.raises(RuntimeError, match="reverse geocode failed for 1 of 2"):
            geocode_map([(1.0, 2.0), (3.0, 4.0)], output=path)

        assert load_cache(path) == {"1.0000,2.0000": ("US", "MT")}

    @patch("utils.reverse_geocode.reverse_geocode")
    def test_all_failed_fetch_raises_without_writing(self, mock_lookup, tmp_path):
        path = str(tmp_path / "cache.csv")
        mock_lookup.side_effect = OSError("no network")

        with pytest.raises(RuntimeError, match="reverse geocode failed for 1 of 1"):
            geocode_map([(1.0, 2.0)], output=path)

        assert not (tmp_path / "cache.csv").exists()

    @patch("utils.reverse_geocode.reverse_geocode")
    def test_no_points_returns_the_cache_without_writing(self, mock_lookup, tmp_path):
        path = str(tmp_path / "cache.csv")

        assert geocode_map([], output=path) == {}

        mock_lookup.assert_not_called()
        assert not (tmp_path / "cache.csv").exists()
