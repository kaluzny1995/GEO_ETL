import pytest

from factory.raw2plain_script_factory import Raw2PlainScriptFactory


@pytest.fixture(autouse=True)
def _clean_registry():
    Raw2PlainScriptFactory.clear()
    yield
    Raw2PlainScriptFactory.clear()


def test_register_and_get():
    def fn(spark, config, db_config):
        return None

    Raw2PlainScriptFactory.register("my_table", fn)
    assert Raw2PlainScriptFactory.get("my_table") is fn


def test_get_unknown_raises():
    with pytest.raises(ValueError, match="No raw2plain script for 'unknown'"):
        Raw2PlainScriptFactory.get("unknown")


def test_clear_empties_registry():
    Raw2PlainScriptFactory.register("t1", lambda s, c, d: None)
    Raw2PlainScriptFactory.clear()
    assert Raw2PlainScriptFactory.tables() == []


def test_register_overwrites_existing():
    def fn1(s, c, d):
        return None

    def fn2(s, c, d):
        return None

    Raw2PlainScriptFactory.register("t", fn1)
    Raw2PlainScriptFactory.register("t", fn2)
    assert Raw2PlainScriptFactory.get("t") is fn2


def test_tables_returns_registered_names():
    Raw2PlainScriptFactory.register("a", lambda s, c, d: None)
    Raw2PlainScriptFactory.register("b", lambda s, c, d: None)
    assert sorted(Raw2PlainScriptFactory.tables()) == ["a", "b"]


def test_tables_empty_when_nothing_registered():
    assert Raw2PlainScriptFactory.tables() == []
