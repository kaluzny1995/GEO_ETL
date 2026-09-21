import pytest
from pydantic import BaseModel

from utils.ddl_generator import pydantic_to_ddl


class SimpleModel(BaseModel):
    name: str
    value: str


class OptionalModel(BaseModel):
    name: str
    tag: str | None = None


class AllOptionalModel(BaseModel):
    name: str | None = None
    tag: str | None = None


def test_simple_ddl():
    ddl = pydantic_to_ddl(SimpleModel, "test_table")
    assert "CREATE TABLE IF NOT EXISTS raw.test_table" in ddl
    assert "name TEXT NOT NULL" in ddl
    assert "value TEXT NOT NULL" in ddl


def test_optional_nullable():
    ddl = pydantic_to_ddl(OptionalModel, "test_table")
    assert "name TEXT NOT NULL" in ddl
    assert "tag TEXT NULL" in ddl


def test_all_optional():
    ddl = pydantic_to_ddl(AllOptionalModel, "test_table")
    assert "name TEXT NULL" in ddl
    assert "tag TEXT NULL" in ddl


def test_custom_schema():
    ddl = pydantic_to_ddl(SimpleModel, "test_table", schema="geo")
    assert "geo.test_table" in ddl


def test_default_schema_is_raw():
    ddl = pydantic_to_ddl(SimpleModel, "test_table")
    assert "raw.test_table" in ddl


def test_ddl_has_parentheses():
    ddl = pydantic_to_ddl(SimpleModel, "test_table")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS raw.test_table (")
    assert ddl.rstrip().endswith(");")


def test_unsupported_type_raises():
    class BadModel(BaseModel):
        data: bytes

    with pytest.raises(TypeError, match="Unsupported type 'bytes'"):
        pydantic_to_ddl(BadModel, "test_table")
