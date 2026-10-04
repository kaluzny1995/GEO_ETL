import pytest

from factory.single_etl_factory import SingleEtlFactory


@pytest.fixture(autouse=True)
def _clean_registry():
    SingleEtlFactory.clear()
    yield
    SingleEtlFactory.clear()


def test_register_and_get():
    def fn(spark, config, db_config):
        return "done"

    SingleEtlFactory.register("my_etl", fn)
    assert SingleEtlFactory.get("my_etl") is fn


def test_get_unknown_raises():
    with pytest.raises(ValueError, match="No single ETL script for 'unknown'"):
        SingleEtlFactory.get("unknown")


def test_clear_empties_registry():
    SingleEtlFactory.register("t1", lambda s, c, d: "done")
    SingleEtlFactory.clear()
    assert SingleEtlFactory.scripts() == []


def test_register_overwrites_existing():
    def fn1(s, c, d):
        return "one"

    def fn2(s, c, d):
        return "two"

    SingleEtlFactory.register("t", fn1)
    SingleEtlFactory.register("t", fn2)
    assert SingleEtlFactory.get("t") is fn2


def test_scripts_returns_registered_names():
    SingleEtlFactory.register("a", lambda s, c, d: "done")
    SingleEtlFactory.register("b", lambda s, c, d: "done")
    assert sorted(SingleEtlFactory.scripts()) == ["a", "b"]


def test_scripts_empty_when_nothing_registered():
    assert SingleEtlFactory.scripts() == []
