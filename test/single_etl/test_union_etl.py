"""The ``union_etl`` script — a regular SINGLE_ETL script that stacks tables."""

from single_etl.union_etl import UnionEtlScript, _cfg

EXPECTED_SOURCES = (
    "avalanche_org_etl",
    "eonet_event_etl",
    "gdacs_event_etl",
    "ibtracs_cyclone_etl",
    "nifc_fire_etl",
    "openmeteo_flood_etl",
    "usgs_earthquake_etl",
    "usgs_nwis_etl",
    "usgs_volcano_etl",
)


def test_source_tables_are_the_registered_etl_tables():
    from factory import TableFactory

    assert UnionEtlScript.SOURCE_TABLE == EXPECTED_SOURCES
    for name in UnionEtlScript.SOURCE_TABLE:
        assert TableFactory.get(name)


def test_target_table_comes_from_the_config():
    assert UnionEtlScript.TARGET_TABLE == "union_etl"
    assert _cfg.target_table == UnionEtlScript.TARGET_TABLE
    assert _cfg.source_tables == list(UnionEtlScript.SOURCE_TABLE)


def test_convert_is_the_identity():
    frame = object()
    assert UnionEtlScript()._convert(frame) is frame


def test_registered_in_the_single_etl_factory():
    import jobs
    from factory import SingleEtlFactory

    # Earlier tests clear the registry — re-run the import-time registration.
    SingleEtlFactory.clear()
    jobs._register()

    script = SingleEtlFactory.get("union_etl")
    assert isinstance(script, UnionEtlScript)
    assert script.TARGET_TABLE in SingleEtlFactory.scripts()
