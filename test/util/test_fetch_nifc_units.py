import io
import json
from unittest.mock import patch

import pytest

from utils.fetch_nifc_units import (
    _fetch_pairs,
    _name_by_code,
    build_lookup,
    load_unit_names,
    unit_name_map,
)

PAIRS = {
    ("WAOWF", "Okanogan-Wenatchee National Forest"),
    ("CAYNP", "Yosemite National Park"),
    ("WANES", "Colville National Forest"),
    ("WANES", "Alpine Lakes Wilderness"),
}


class _Response(io.BytesIO):
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class TestFetchPairs:
    @staticmethod
    def _payload():
        features = [
            {"attributes": {"JurisdictionalUnitID_sansUS": code, "JurisdictionalUnitName": name}}
            for code, name in PAIRS
        ]
        return {"features": features}

    def test_returns_distinct_pairs(self):
        with patch(
            "utils.fetch_nifc_units.urllib.request.urlopen",
            return_value=_Response(json.dumps(self._payload()).encode()),
        ) as mocked:
            pairs = _fetch_pairs("https://example.test/query")

        assert pairs == PAIRS
        assert "returnDistinctValues=true" in mocked.call_args.args[0]
        assert "JurisdictionalUnitID_sansUS+IS+NOT+NULL" in mocked.call_args.args[0]

    def test_skips_blank_codes_and_names(self):
        payload = {
            "features": [
                {"attributes": {"JurisdictionalUnitID_sansUS": "WAOWF", "JurisdictionalUnitName": "Ok forests"}},
                {"attributes": {"JurisdictionalUnitID_sansUS": "", "JurisdictionalUnitName": "Private parcel"}},
                {"attributes": {"JurisdictionalUnitID_sansUS": None, "JurisdictionalUnitName": "Private parcel 2"}},
                {"attributes": {"JurisdictionalUnitID_sansUS": "CAYNP", "JurisdictionalUnitName": ""}},
                {"attributes": {}},
            ]
        }
        with patch(
            "utils.fetch_nifc_units.urllib.request.urlopen",
            return_value=_Response(json.dumps(payload).encode()),
        ):
            pairs = _fetch_pairs("https://example.test/query")

        assert pairs == {("WAOWF", "Ok forests")}

    def test_error_payload_raises(self):
        payload = {"error": {"code": 400, "message": "Invalid query."}}
        with (
            patch(
                "utils.fetch_nifc_units.urllib.request.urlopen",
                return_value=_Response(json.dumps(payload).encode()),
            ),
            pytest.raises(RuntimeError, match="rejected the query: Invalid query"),
        ):
            _fetch_pairs("https://example.test/query")

    def test_empty_features_raise(self):
        with (
            patch(
                "utils.fetch_nifc_units.urllib.request.urlopen",
                return_value=_Response(json.dumps({"features": []}).encode()),
            ),
            pytest.raises(RuntimeError, match="no code/name pairs"),
        ):
            _fetch_pairs("https://example.test/query")


class TestNameByCode:
    def test_one_name_per_code_first_sorted_wins(self):
        names = _name_by_code(PAIRS)

        assert names == {
            "WAOWF": "Okanogan-Wenatchee National Forest",
            "CAYNP": "Yosemite National Park",
            "WANES": "Alpine Lakes Wilderness",
        }


class TestLoadUnitNames:
    def test_missing_file_returns_empty_map(self, tmp_path):
        assert load_unit_names(str(tmp_path / "nope.csv")) == {}

    def test_reads_the_cache_csv(self, tmp_path):
        output = tmp_path / "nifc_unit_names.csv"
        output.write_text(
            "unit_code,unit_name\nWAOWF,Okanogan-Wenatchee National Forest\nCAYNP,Yosemite National Park\n"
        )

        assert load_unit_names(str(output)) == {
            "WAOWF": "Okanogan-Wenatchee National Forest",
            "CAYNP": "Yosemite National Park",
        }

    def test_blank_rows_are_skipped(self, tmp_path):
        output = tmp_path / "nifc_unit_names.csv"
        output.write_text("unit_code,unit_name\n,\nWAOWF,\nCAYNP,Yosemite\n")

        assert load_unit_names(str(output)) == {"CAYNP": "Yosemite"}


class TestBuildLookup:
    @staticmethod
    def _fetch(_url):
        return PAIRS

    def test_writes_the_csv_and_creates_the_directory(self, tmp_path, monkeypatch):
        monkeypatch.setattr("utils.fetch_nifc_units._fetch_pairs", self._fetch)
        output = tmp_path / "cache" / "nifc_unit_names.csv"

        written = build_lookup(output=str(output))

        assert written == 3
        assert output.read_text().splitlines() == [
            "unit_code,unit_name",
            "CAYNP,Yosemite National Park",
            "WANES,Alpine Lakes Wilderness",
            "WAOWF,Okanogan-Wenatchee National Forest",
        ]

    def test_fetch_failure_writes_nothing(self, tmp_path, monkeypatch):
        def boom(_url):
            raise RuntimeError("NIFC jurisdiction layer returned no code/name pairs — nothing written")

        monkeypatch.setattr("utils.fetch_nifc_units._fetch_pairs", boom)
        output = tmp_path / "nifc_unit_names.csv"

        with pytest.raises(RuntimeError, match="no code/name pairs"):
            build_lookup(output=str(output))

        assert not output.exists()


class TestUnitNameMap:
    @staticmethod
    def _write_cache(tmp_path, codes=("WAOWF", "CAYNP")):
        output = tmp_path / "nifc_unit_names.csv"
        rows = "".join(f"{code},Name of {code}\n" for code in codes)
        output.write_text(f"unit_code,unit_name\n{rows}")
        return str(output)

    def test_cache_hit_does_not_fetch(self, tmp_path, monkeypatch):
        output = self._write_cache(tmp_path)
        monkeypatch.setattr(
            "utils.fetch_nifc_units.build_lookup",
            lambda *args, **kwargs: pytest.fail("fetch not expected on a cache hit"),
        )

        assert unit_name_map({"WAOWF", "CAYNP"}, output=output) == {
            "WAOWF": "Name of WAOWF",
            "CAYNP": "Name of CAYNP",
        }

    def test_unknown_code_triggers_a_refresh(self, tmp_path, monkeypatch):
        output = self._write_cache(tmp_path)

        def refresh(api_url=..., output=...):
            with open(output, "w") as handle:
                handle.write("unit_code,unit_name\nWAOWF,Name of WAOWF\nCAYNP,Name of CAYNP\nZZZZZ,Fresh Name\n")
            return 3

        monkeypatch.setattr("utils.fetch_nifc_units.build_lookup", refresh)

        assert unit_name_map({"WAOWF", "ZZZZZ"}, output=output)["ZZZZZ"] == "Fresh Name"

    def test_unresolvable_code_is_dropped_not_raised(self, tmp_path, monkeypatch):
        output = self._write_cache(tmp_path)

        def refresh(api_url=..., output=...):
            return 2

        monkeypatch.setattr("utils.fetch_nifc_units.build_lookup", refresh)

        assert unit_name_map({"WAOWF", "NVNV0100"}, output=output) == {"WAOWF": "Name of WAOWF"}

    def test_blank_codes_are_ignored(self, tmp_path, monkeypatch):
        output = self._write_cache(tmp_path, codes=("WAOWF",))
        monkeypatch.setattr(
            "utils.fetch_nifc_units.build_lookup",
            lambda *args, **kwargs: pytest.fail("blank codes must not trigger a fetch"),
        )

        assert unit_name_map({"", "   "}, output=output) == {}

    def test_missing_cache_is_built(self, tmp_path, monkeypatch):
        output = str(tmp_path / "nifc_unit_names.csv")

        def refresh(api_url=..., output=...):
            with open(output, "w") as handle:
                handle.write("unit_code,unit_name\nWAOWF,Name of WAOWF\n")
            return 1

        monkeypatch.setattr("utils.fetch_nifc_units.build_lookup", refresh)

        assert unit_name_map({"WAOWF"}, output=output) == {"WAOWF": "Name of WAOWF"}
