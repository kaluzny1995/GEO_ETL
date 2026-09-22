import pytest
from factory.upload_script_factory import UploadScriptFactory


@pytest.fixture(autouse=True)
def _clean_registry():
    UploadScriptFactory.clear()
    yield
    UploadScriptFactory.clear()


def test_register_and_get():
    fn = lambda spark, config, db_config: None
    UploadScriptFactory.register("my_table", fn)
    assert UploadScriptFactory.get("my_table") is fn


def test_get_unknown_raises():
    with pytest.raises(ValueError, match="No upload script for 'unknown'"):
        UploadScriptFactory.get("unknown")


def test_clear_empties_registry():
    UploadScriptFactory.register("t1", lambda s, c, d: None)
    UploadScriptFactory.clear()
    assert UploadScriptFactory.tables() == []


def test_register_overwrites_existing():
    fn1 = lambda s, c, d: None
    fn2 = lambda s, c, d: None
    UploadScriptFactory.register("t", fn1)
    UploadScriptFactory.register("t", fn2)
    assert UploadScriptFactory.get("t") is fn2


def test_tables_returns_registered_names():
    UploadScriptFactory.register("a", lambda s, c, d: None)
    UploadScriptFactory.register("b", lambda s, c, d: None)
    assert sorted(UploadScriptFactory.tables()) == ["a", "b"]


def test_tables_empty_when_nothing_registered():
    assert UploadScriptFactory.tables() == []
