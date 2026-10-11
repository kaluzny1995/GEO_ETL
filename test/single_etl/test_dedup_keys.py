"""Contract tests for the ``DEDUP_KEYS`` every single-ETL script declares."""

import pytest

from model.etl.event_etl import EventEtl
from single_etl.avalanche_org_etl import AvalancheOrgEtlScript
from single_etl.eonet_event_etl import EonetEventEtlScript
from single_etl.gdacs_event_etl import GdacsEventEtlScript
from single_etl.ibtracs_cyclone_etl import IbtracsCycloneEtlScript
from single_etl.nifc_fire_etl import NifcFireEtlScript
from single_etl.openmeteo_flood_etl import OpenMeteoFloodEtlScript
from single_etl.union_etl import UnionEtlScript
from single_etl.usgs_earthquake_etl import UsgsEarthquakeEtlScript
from single_etl.usgs_nwis_etl import UsgsNwisEtlScript
from single_etl.usgs_volcano_etl import UsgsVolcanoEtlScript

SOURCE_SCRIPTS = [
    AvalancheOrgEtlScript,
    EonetEventEtlScript,
    GdacsEventEtlScript,
    IbtracsCycloneEtlScript,
    NifcFireEtlScript,
    OpenMeteoFloodEtlScript,
    UsgsEarthquakeEtlScript,
    UsgsNwisEtlScript,
    UsgsVolcanoEtlScript,
]

CANONICAL_COLUMNS = set(EventEtl.model_fields)


class TestDedupKeys:
    @pytest.mark.parametrize("script_cls", SOURCE_SCRIPTS)
    def test_source_scripts_declare_dedup_keys(self, script_cls):
        assert script_cls.DEDUP_KEYS, (
            f"{script_cls.__name__} must declare DEDUP_KEYS — every source table "
            "deduplicates its converted rows within each file_date"
        )

    @pytest.mark.parametrize("script_cls", SOURCE_SCRIPTS)
    def test_keys_are_canonical_columns(self, script_cls):
        keys = set(script_cls.DEDUP_KEYS)

        assert keys <= CANONICAL_COLUMNS, f"{script_cls.__name__} uses non-canonical keys: {keys - CANONICAL_COLUMNS}"

    @pytest.mark.parametrize("script_cls", SOURCE_SCRIPTS)
    def test_keys_leave_the_file_date_scope_to_the_runner(self, script_cls):
        keys = script_cls.DEDUP_KEYS

        assert "file_date" not in keys, f"{script_cls.__name__} must not include file_date — the runner adds it"
        assert len(set(keys)) == len(keys), f"{script_cls.__name__} repeats a dedup key: {keys}"

    def test_union_stacks_sources_that_are_already_deduplicated(self):
        assert UnionEtlScript.DEDUP_KEYS is None
