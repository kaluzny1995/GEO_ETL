import pytest
from factory.raw2final_script_factory import Raw2FinalScriptFactory


@pytest.fixture(autouse=True)
def _clean_registry():
    Raw2FinalScriptFactory.clear()
    yield
    Raw2FinalScriptFactory.clear()


def test_register_and_get():
    fn = lambda spark, config, db_config: None
    Raw2FinalScriptFactory.register("my_table", fn)
    assert Raw2FinalScriptFactory.get("my_table") is fn


def test_get_unknown_raises():
    with pytest.raises(ValueError, match="No raw2final script for 'unknown'"):
        Raw2FinalScriptFactory.get("unknown")


def test_clear_empties_registry():
    Raw2FinalScriptFactory.register("t1", lambda s, c, d: None)
    Raw2FinalScriptFactory.clear()
    assert Raw2FinalScriptFactory.tables() == []


def test_register_overwrites_existing():
    fn1 = lambda s, c, d: None
    fn2 = lambda s, c, d: None
    Raw2FinalScriptFactory.register("t", fn1)
    Raw2FinalScriptFactory.register("t", fn2)
    assert Raw2FinalScriptFactory.get("t") is fn2


def test_tables_returns_registered_names():
    Raw2FinalScriptFactory.register("a", lambda s, c, d: None)
    Raw2FinalScriptFactory.register("b", lambda s, c, d: None)
    assert sorted(Raw2FinalScriptFactory.tables()) == ["a", "b"]


def test_tables_empty_when_nothing_registered():
    assert Raw2FinalScriptFactory.tables() == []
