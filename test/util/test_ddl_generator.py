import datetime as dt
from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from utils.ddl_generator import _schema_has_tables, create_schema, drop_schema, drop_table, execute_ddl, pydantic_to_ddl


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


def test_int_type():
    class IntModel(BaseModel):
        count: int

    ddl = pydantic_to_ddl(IntModel, "test_table", schema="raw")
    assert "count INTEGER NOT NULL" in ddl


def test_float_type():
    class FloatModel(BaseModel):
        score: float

    ddl = pydantic_to_ddl(FloatModel, "test_table", schema="raw")
    assert "score DOUBLE PRECISION NOT NULL" in ddl


def test_bool_type():
    class BoolModel(BaseModel):
        active: bool

    ddl = pydantic_to_ddl(BoolModel, "test_table", schema="raw")
    assert "active BOOLEAN NOT NULL" in ddl


def test_datetime_type():
    class DtModel(BaseModel):
        created_at: dt.datetime

    ddl = pydantic_to_ddl(DtModel, "test_table", schema="raw")
    assert "created_at TIMESTAMP NOT NULL" in ddl


def test_date_type():
    class DateModel(BaseModel):
        birth_date: dt.date

    ddl = pydantic_to_ddl(DateModel, "test_table", schema="raw")
    assert "birth_date DATE NOT NULL" in ddl


def test_mixed_types():
    class MixedModel(BaseModel):
        id: int
        name: str
        active: bool
        score: float
        created_at: dt.datetime
        birth_date: dt.date

    ddl = pydantic_to_ddl(MixedModel, "test_table", schema="raw")
    assert "id INTEGER NOT NULL" in ddl
    assert "name TEXT NOT NULL" in ddl
    assert "active BOOLEAN NOT NULL" in ddl
    assert "score DOUBLE PRECISION NOT NULL" in ddl
    assert "created_at TIMESTAMP NOT NULL" in ddl
    assert "birth_date DATE NOT NULL" in ddl


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


@patch("utils.ddl_generator._get_jdbc_connection")
def test_create_schema(mock_get_conn):
    conn, stmt = _mock_conn()
    mock_get_conn.return_value = conn

    mock_spark = MagicMock()
    create_schema(mock_spark, "geo", "jdbc:url", {"user": "u", "password": "p"})

    stmt.execute.assert_called_once_with("CREATE SCHEMA IF NOT EXISTS geo;")
    conn.close.assert_called_once()


@patch("utils.ddl_generator._get_jdbc_connection")
def test_execute_ddl(mock_get_conn):
    conn, stmt = _mock_conn()
    mock_get_conn.return_value = conn

    mock_spark = MagicMock()
    execute_ddl(mock_spark, "CREATE TABLE t (id INT);", "jdbc:url", {"user": "u", "password": "p"})

    stmt.execute.assert_called_once_with("CREATE TABLE t (id INT);")
    conn.close.assert_called_once()
