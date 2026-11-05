"""Parametrized tests shared by the three script factories.

Each case exercises the same behaviour against a different factory class —
Upload exposes ``tables()``, SingleETL and View expose ``scripts()``,
and each factory raises its own error message for an unknown key.
"""

import pytest

from factory.single_etl_factory import SingleEtlFactory
from factory.upload_script_factory import UploadScriptFactory
from factory.view_script_factory import ViewScriptFactory

# (factory, list method, unknown-key error, register return, overwrite first, overwrite second)
FACTORY_CASES = [
    (UploadScriptFactory, "tables", "No upload script for 'unknown'", None, None, None),
    (SingleEtlFactory, "scripts", "No single ETL script for 'unknown'", "done", "one", "two"),
    (ViewScriptFactory, "scripts", "No view script for 'unknown'", "done", "one", "two"),
]

FACTORIES = [factory for factory, _method, _msg, _base, _first, _second in FACTORY_CASES]


@pytest.fixture(autouse=True)
def _clean_registry():
    for factory in FACTORIES:
        factory.clear()
    yield
    for factory in FACTORIES:
        factory.clear()


@pytest.mark.parametrize("factory, method, unknown_error, value, first, second", FACTORY_CASES)
def test_register_and_get(factory, method, unknown_error, value, first, second):
    # Arrange
    def fn(spark, config, db_config):
        return value

    # Act
    factory.register("my_etl", fn)

    # Assert
    assert factory.get("my_etl") is fn
    assert factory.get("my_etl")(None, None, None) == value


@pytest.mark.parametrize("factory, method, unknown_error, value, first, second", FACTORY_CASES)
def test_get_unknown_raises(factory, method, unknown_error, value, first, second):
    # Assert
    with pytest.raises(ValueError, match=unknown_error):
        factory.get("unknown")


@pytest.mark.parametrize("factory, method, unknown_error, value, first, second", FACTORY_CASES)
def test_clear_empties_registry(factory, method, unknown_error, value, first, second):
    # Arrange
    factory.register("t1", lambda s, c, d: value)

    # Act
    factory.clear()

    # Assert
    assert getattr(factory, method)() == []


@pytest.mark.parametrize("factory, method, unknown_error, value, first, second", FACTORY_CASES)
def test_register_overwrites_existing(factory, method, unknown_error, value, first, second):
    # Arrange
    def fn1(s, c, d):
        return first

    def fn2(s, c, d):
        return second

    # Act / Assert
    factory.register("t", fn1)
    assert factory.get("t") is fn1
    assert factory.get("t")(None, None, None) == first
    factory.register("t", fn2)
    assert factory.get("t") is fn2
    assert factory.get("t")(None, None, None) == second


@pytest.mark.parametrize("factory, method, unknown_error, value, first, second", FACTORY_CASES)
def test_names_returns_registered_names(factory, method, unknown_error, value, first, second):
    # Arrange
    factory.register("a", lambda s, c, d: value)
    factory.register("b", lambda s, c, d: value)

    # Assert
    assert sorted(getattr(factory, method)()) == ["a", "b"]


@pytest.mark.parametrize("factory, method, unknown_error, value, first, second", FACTORY_CASES)
def test_names_empty_when_nothing_registered(factory, method, unknown_error, value, first, second):
    # Assert
    assert getattr(factory, method)() == []
