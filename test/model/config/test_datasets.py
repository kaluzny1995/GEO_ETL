import json
import os
import tempfile

import pytest
from pydantic import ValidationError

from model.config.app_config import CONFIG_PATH
from model.config.datasets import (
    AvalancheOrgEtlConfig,
    EonetEventEtlConfig,
    GdacsEventEtlConfig,
    Raw2PlainDatasetConfig,
    UnionEtlConfig,
    UploadDatasetConfig,
    UsgsVolcanoEtlConfig,
    ViewDatasetConfig,
    load_dataset,
)
from model.enums import DangerRating

DATASETS = {
    "upload": {"demo_upload": {"table_name": "demo_raw", "dataset_folder": "Demo"}},
    "raw2plain": {"demo_raw2plain": {"raw_table": "demo_raw", "plain_table": "demo"}},
    "single_etl": {
        "demo_etl": {
            "source_table": "demo",
            "target_table": "demo_etl",
            "source_name": "DEMO",
            "dedup_keys": ["event_name", "latitude", "longitude", "event_start"],
            "category_map": {"A": "alpha"},
        },
        "demo_union_etl": {"target_table": "demo_union_etl", "source_tables": ["alpha_etl", "beta_etl"]},
    },
    "view": {"demo_vw": {"source_table": "demo_etl", "target_view": "demo_vw", "query": "SELECT * FROM geo.demo_etl"}},
}


def _write_config(datasets: dict) -> str:
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump({"datasets": datasets}, f)
        f.flush()
    return f.name


@pytest.fixture()
def config_path():
    path = _write_config(DATASETS)
    yield path
    os.unlink(path)


class TestLoadDataset:
    def test_returns_typed_upload_config(self, config_path):
        cfg = load_dataset("/x/demo_upload.py", "upload", UploadDatasetConfig, path=config_path)
        assert cfg.table_name == "demo_raw"
        assert cfg.dataset_folder == "Demo"

    def test_returns_typed_raw2plain_config(self, config_path):
        cfg = load_dataset("/x/demo_raw2plain.py", "raw2plain", Raw2PlainDatasetConfig, path=config_path)
        assert cfg.raw_table == "demo_raw"
        assert cfg.plain_table == "demo"

    def test_key_is_the_script_file_stem(self, config_path):
        cfg = load_dataset("single_etl/demo_etl.py", "single_etl", EonetEventEtlConfig, path=config_path)
        assert cfg.source_table == "demo"
        assert cfg.target_table == "demo_etl"
        assert cfg.dedup_keys == ["event_name", "latitude", "longitude", "event_start"]
        assert cfg.category_map == {"A": "alpha"}

    @pytest.mark.parametrize(
        ("dedup_keys", "message"),
        [
            ([], "must not be empty"),
            (["event_name", "event_name"], "must be unique"),
            (["event_name", "file_date"], "must not include file_date"),
            (["event_name", " "], "must not contain blank"),
        ],
    )
    def test_invalid_dedup_keys_are_rejected(self, dedup_keys, message):
        path = _write_config(
            {
                "single_etl": {
                    "demo_etl": {
                        "source_table": "demo",
                        "target_table": "demo_etl",
                        "source_name": "DEMO",
                        "dedup_keys": dedup_keys,
                        "category_map": {"A": "alpha"},
                    }
                }
            }
        )
        try:
            with pytest.raises(ValidationError, match=message):
                load_dataset("/x/demo_etl.py", "single_etl", EonetEventEtlConfig, path=path)
        finally:
            os.unlink(path)

    def test_view_country_codes_are_optional(self, config_path):
        cfg = load_dataset("/x/demo_vw.py", "view", ViewDatasetConfig, path=config_path)
        assert cfg.country_codes == []

    def test_view_country_codes_are_loaded(self):
        path = _write_config(
            {
                "view": {
                    "demo_vw": {
                        "source_table": "demo_etl",
                        "target_view": "demo_vw",
                        "query": "SELECT * FROM geo.demo_etl WHERE country_code IN ('CA','US')",
                        "country_codes": ["CA", "US"],
                    }
                }
            }
        )
        try:
            cfg = load_dataset("/x/demo_vw.py", "view", ViewDatasetConfig, path=path)
            assert cfg.country_codes == ["CA", "US"]
        finally:
            os.unlink(path)

    @pytest.mark.parametrize(
        ("country_codes", "message"),
        [
            (["US", "US"], "must be unique"),
            (["US", "CA"], "must be sorted"),
            (["us"], "two-letter upper-case"),
            (["USA"], "two-letter upper-case"),
            (["U1"], "two-letter upper-case"),
            (["ÜS"], "two-letter upper-case"),
        ],
    )
    def test_invalid_country_codes_are_rejected(self, country_codes, message):
        path = _write_config(
            {
                "view": {
                    "demo_vw": {
                        "source_table": "demo_etl",
                        "target_view": "demo_vw",
                        "query": "SELECT * FROM geo.demo_etl",
                        "country_codes": country_codes,
                    }
                }
            }
        )
        try:
            with pytest.raises(ValidationError, match=message):
                load_dataset("/x/demo_vw.py", "view", ViewDatasetConfig, path=path)
        finally:
            os.unlink(path)

    def test_returns_typed_union_config(self, config_path):
        cfg = load_dataset("/x/demo_union_etl.py", "single_etl", UnionEtlConfig, path=config_path)
        assert cfg.target_table == "demo_union_etl"
        assert cfg.source_tables == ["alpha_etl", "beta_etl"]

    def test_returns_typed_view_config(self, config_path):
        cfg = load_dataset("/x/demo_vw.py", "view", ViewDatasetConfig, path=config_path)
        assert cfg.source_table == "demo_etl"
        assert cfg.target_view == "demo_vw"
        assert cfg.query == "SELECT * FROM geo.demo_etl"

    def test_view_config_missing_query_is_rejected(self, config_path):
        path = _write_config({"view": {"demo_vw": {"source_table": "demo_etl", "target_view": "demo_vw"}}})
        try:
            with pytest.raises(ValidationError):
                load_dataset("/x/demo_vw.py", "view", ViewDatasetConfig, path=path)
        finally:
            os.unlink(path)

    def test_unknown_key_raises_key_error(self, config_path):
        with pytest.raises(KeyError, match=r"datasets\.upload"):
            load_dataset("/x/missing_upload.py", "upload", UploadDatasetConfig, path=config_path)

    def test_unknown_section_raises_key_error(self, config_path):
        with pytest.raises(KeyError, match=r"datasets\.nope"):
            load_dataset("/x/demo_upload.py", "nope", UploadDatasetConfig, path=config_path)  # type: ignore[arg-type]

    def test_extra_fields_are_rejected(self, config_path):
        with pytest.raises(ValidationError):
            load_dataset("/x/demo_upload.py", "upload", Raw2PlainDatasetConfig, path=config_path)

    def test_missing_field_is_rejected(self, config_path):
        path = _write_config({"upload": {"demo_upload": {"table_name": "demo_raw"}}})
        try:
            with pytest.raises(ValidationError):
                load_dataset("/x/demo_upload.py", "upload", UploadDatasetConfig, path=path)
        finally:
            os.unlink(path)

    def test_config_is_frozen(self, config_path):
        cfg = load_dataset("/x/demo_upload.py", "upload", UploadDatasetConfig, path=config_path)
        with pytest.raises(ValidationError):
            cfg.table_name = "other"  # type: ignore[misc]


class TestProjectConfig:
    def test_project_config_has_all_dataset_sections(self):
        with open(CONFIG_PATH) as f:
            datasets = json.load(f)["datasets"]
        assert set(datasets) == {"upload", "raw2plain", "single_etl", "view"}
        assert len(datasets["upload"]) == 9
        assert len(datasets["raw2plain"]) == 9
        assert len(datasets["single_etl"]) == 10
        assert set(datasets["view"]) == {
            "metrics_avalanche_vw",
            "metrics_drought_vw",
            "metrics_earthquake_vw",
            "metrics_fire_vw",
            "metrics_flood_vw",
            "metrics_iceberg_vw",
            "metrics_storm_vw",
            "metrics_volcano_vw",
            "phenomenons_us_vw",
            "phenomenons_ca_vw",
            "phenomenons_cs_amer_vw",
            "phenomenons_ru_vw",
            "phenomenons_europe_vw",
            "phenomenons_asia_vw",
            "phenomenons_africa_vw",
            "phenomenons_aus_oc_vw",
            "phenomenons_waters_vw",
        }

    def test_project_union_script_config_loads(self):
        cfg = load_dataset("/x/union_etl.py", "single_etl", UnionEtlConfig)
        assert cfg.target_table == "union_etl"
        assert cfg.source_tables == [
            "avalanche_org_etl",
            "eonet_event_etl",
            "gdacs_event_etl",
            "ibtracs_cyclone_etl",
            "nifc_fire_etl",
            "openmeteo_flood_etl",
            "usgs_earthquake_etl",
            "usgs_nwis_etl",
            "usgs_volcano_etl",
        ]

    def test_project_config_sections_load(self):
        for key, cfg in {
            "avalanche_org_upload": UploadDatasetConfig,
            "avalanche_org_raw2plain": Raw2PlainDatasetConfig,
            "gdacs_event_etl": GdacsEventEtlConfig,
            "union_etl": UnionEtlConfig,
            "metrics_avalanche_vw": ViewDatasetConfig,
            "metrics_drought_vw": ViewDatasetConfig,
            "metrics_earthquake_vw": ViewDatasetConfig,
            "metrics_fire_vw": ViewDatasetConfig,
            "metrics_flood_vw": ViewDatasetConfig,
            "metrics_iceberg_vw": ViewDatasetConfig,
            "metrics_storm_vw": ViewDatasetConfig,
            "metrics_volcano_vw": ViewDatasetConfig,
            "phenomenons_us_vw": ViewDatasetConfig,
            "phenomenons_ca_vw": ViewDatasetConfig,
            "phenomenons_cs_amer_vw": ViewDatasetConfig,
            "phenomenons_ru_vw": ViewDatasetConfig,
            "phenomenons_europe_vw": ViewDatasetConfig,
            "phenomenons_asia_vw": ViewDatasetConfig,
            "phenomenons_africa_vw": ViewDatasetConfig,
            "phenomenons_aus_oc_vw": ViewDatasetConfig,
            "phenomenons_waters_vw": ViewDatasetConfig,
        }.items():
            if key.endswith("_etl"):
                section = "single_etl"
            elif key.endswith("_raw2plain"):
                section = "raw2plain"
            elif key.endswith("_vw"):
                section = "view"
            else:
                section = "upload"
            load_dataset(f"/x/{key}.py", section, cfg)


class TestDangerRatedEtlConfig:
    def test_project_avalanche_map_is_typed(self):
        cfg = load_dataset("/x/avalanche_org_etl.py", "single_etl", AvalancheOrgEtlConfig)

        assert cfg.danger_rating_map == {
            "1": DangerRating.LOW,
            "2": DangerRating.MEDIUM,
            "3": DangerRating.HIGH,
            "4": DangerRating.HIGH,
            "5": DangerRating.EXTREME,
        }

    def test_project_volcano_has_a_map_per_scale(self):
        cfg = load_dataset("/x/usgs_volcano_etl.py", "single_etl", UsgsVolcanoEtlConfig)

        assert cfg.danger_rating_map["watch"] is DangerRating.HIGH
        assert cfg.color_danger_rating_map["red"] is DangerRating.EXTREME

    def test_an_unknown_word_in_the_map_is_rejected(self):
        datasets = {
            "single_etl": {
                "demo_etl": {
                    "source_table": "demo",
                    "target_table": "demo_etl",
                    "source_name": "DEMO",
                    "category_map": {"A": "alpha"},
                    "country_code_map": {"Xanadu": "XA"},
                    "danger_rating_map": {"1": "catastrophic"},
                }
            }
        }
        path = _write_config(datasets)

        try:
            with pytest.raises(ValidationError):
                load_dataset("/x/demo_etl.py", "single_etl", GdacsEventEtlConfig, path=path)
        finally:
            os.unlink(path)
