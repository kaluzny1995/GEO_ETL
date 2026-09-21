import pytest
from pydantic import BaseModel
from unittest.mock import MagicMock, patch

from utils.ddl_generator import pydantic_to_ddl, drop_table, drop_schema, _schema_has_tables


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
    ddl = pydantic_to_ddl(SimpleModel, "test_table", schema="raw")
    assert "CREATE TABLE IF NOT EXISTS raw.test_table" in ddl
    assert "name TEXT NOT NULL" in ddl
    assert "value TEXT NOT NULL" in ddl


def test_optional_nullable():
    ddl = pydantic_to_ddl(OptionalModel, "test_table", schema="raw")
    assert "name TEXT NOT NULL" in ddl
    assert "tag TEXT NULL" in ddl


def test_all_optional():
    ddl = pydantic_to_ddl(AllOptionalModel, "test_table", schema="raw")
    assert "name TEXT NULL" in ddl
    assert "tag TEXT NULL" in ddl


def test_custom_schema():
    ddl = pydantic_to_ddl(SimpleModel, "test_table", schema="geo")
    assert "geo.test_table" in ddl


def test_schema_is_required():
    with pytest.raises(TypeError):
        pydantic_to_ddl(SimpleModel, "test_table")


def test_ddl_has_parentheses():
    ddl = pydantic_to_ddl(SimpleModel, "test_table", schema="raw")
    assert ddl.startswith("CREATE TABLE IF NOT EXISTS raw.test_table (")
    assert ddl.rstrip().endswith(");")


def test_unsupported_type_raises():
    class BadModel(BaseModel):
        data: bytes

    with pytest.raises(TypeError, match="Unsupported type 'bytes'"):
        pydantic_to_ddl(BadModel, "test_table", schema="raw")


def _mock_conn():
    conn = MagicMock()
    stmt = MagicMock()
    conn.createStatement.return_value = stmt
    return conn, stmt


@patch("utils.ddl_generator._get_jdbc_connection")
def test_drop_table(mock_get_conn):
    conn, stmt = _mock_conn()
    mock_get_conn.return_value = conn

    mock_spark = MagicMock()
    drop_table(mock_spark, "my_table", "geo", "jdbc:url", {})

    stmt.execute.assert_called_once_with("DROP TABLE IF EXISTS geo.my_table;")
    conn.close.assert_called_once()


@patch("utils.ddl_generator._schema_has_tables", return_value=False)
@patch("utils.ddl_generator._get_jdbc_connection")
def test_drop_schema_when_empty(mock_get_conn, mock_has_tables):
    conn, stmt = _mock_conn()
    mock_get_conn.return_value = conn

    mock_spark = MagicMock()
    drop_schema(mock_spark, "geo", "jdbc:url", {})

    mock_has_tables.assert_called_once()
    stmt.execute.assert_called_once_with("DROP SCHEMA IF EXISTS geo;")
    conn.close.assert_called_once()


@patch("utils.ddl_generator._schema_has_tables", return_value=True)
@patch("utils.ddl_generator._get_jdbc_connection")
def test_drop_schema_when_has_tables(mock_get_conn, mock_has_tables):
    mock_spark = MagicMock()
    drop_schema(mock_spark, "geo", "jdbc:url", {})

    mock_has_tables.assert_called_once()
    mock_get_conn.assert_not_called()


@patch("utils.ddl_generator._query_jdbc", return_value=3)
def test_schema_has_tables_true(mock_query):
    mock_spark = MagicMock()
    result = _schema_has_tables(mock_spark, "geo", "jdbc:url", {})

    assert result is True
    mock_query.assert_called_once()
    sql = mock_query.call_args[0][1]
    assert "information_schema.tables" in sql
    assert "geo" in sql


@patch("utils.ddl_generator._query_jdbc", return_value=0)
def test_schema_has_tables_false(mock_query):
    mock_spark = MagicMock()
    result = _schema_has_tables(mock_spark, "geo", "jdbc:url", {})

    assert result is False
