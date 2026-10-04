from unittest.mock import MagicMock

import pytest
from pydantic import BaseModel

from factory import TableFactory


class DummyModel(BaseModel):
    name: str


@pytest.fixture(autouse=True)
def clean_registry():
    """Clear registry around each test in this module, then restore the real tables."""
    from model import _register_tables

    TableFactory.clear()
    yield
    TableFactory.clear()
    _register_tables()


class TestTableFactoryRegister:
    def test_register_and_get(self):
        TableFactory.register("my_table", DummyModel)
        result = TableFactory.get("my_table")
        assert result is DummyModel

    def test_get_unknown_raises(self):
        with pytest.raises(ValueError, match="Unknown table"):
            TableFactory.get("nonexistent")

    def test_clear_empties_registry(self):
        TableFactory.register("my_table", DummyModel)
        TableFactory.clear()
        with pytest.raises(ValueError):
            TableFactory.get("my_table")

    def test_register_overwrites_existing(self):
        first = MagicMock()
        second = MagicMock()
        TableFactory.register("my_table", first)
        TableFactory.register("my_table", second)
        assert TableFactory.get("my_table") is second

    def test_tables_returns_registered_names(self):
        TableFactory.register("table_a", DummyModel)
        TableFactory.register("table_b", DummyModel)
        assert sorted(TableFactory.tables()) == ["table_a", "table_b"]

    def test_tables_empty_when_nothing_registered(self):
        assert TableFactory.tables() == []
