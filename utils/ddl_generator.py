"""DDL generation and JDBC execution utilities.

Converts Pydantic models to PostgreSQL ``CREATE TABLE`` statements and
provides helpers for schema/table management via direct JDBC connections.
"""

import datetime as dt
import types
from typing import Union, get_args, get_origin

from pydantic import BaseModel

from pyspark.sql import SparkSession


def _resolve_type(annotation: type) -> type:
    """Extract the base type from Optional/Union annotations.

    Args:
        annotation: A Python type annotation, possibly wrapped in
            ``Optional`` or ``Union[..., None]``.

    Returns:
        The unwrapped inner type (e.g. ``str | None`` → ``str``).
    """
    origin = get_origin(annotation)
    if origin is Union or isinstance(annotation, types.UnionType):
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _pg_type(annotation: type) -> str:
    """Map a Python type to its PostgreSQL column type string.

    Args:
        annotation: A resolved Python type (e.g. ``str``, ``int``, ``float``).

    Returns:
        The corresponding PostgreSQL type name.

    Raises:
        TypeError: If the annotation maps to an unsupported PostgreSQL type.
    """
    resolved = _resolve_type(annotation)
    mapping = {
        str: "TEXT",
        int: "INTEGER",
        float: "DOUBLE PRECISION",
        bool: "BOOLEAN",
        dt.datetime: "TIMESTAMP",
        dt.date: "DATE",
    }
    result = mapping.get(resolved)
    if result is None:
        raise TypeError(f"Unsupported type '{resolved.__name__}' for PostgreSQL DDL")
    return result


def pydantic_to_ddl(model: type[BaseModel], table_name: str, schema: str) -> str:
    """Convert a Pydantic model to a ``CREATE TABLE IF NOT EXISTS`` DDL statement.

    Args:
        model: Pydantic model class whose fields define the table columns.
        table_name: Target table name (without schema prefix).
        schema: PostgreSQL schema name (e.g. ``"geo"``).

    Returns:
        A complete DDL string ready for JDBC execution.
    """
    columns = []
    for name, field_info in model.model_fields.items():
        annotation = field_info.annotation
        nullable = "NULL" if field_info.is_required() is False else "NOT NULL"
        pg_type = _pg_type(annotation)
        columns.append(f"    {name} {pg_type} {nullable}")

    columns_ddl = ",\n".join(columns)
    return (
        f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} (\n"
        f"{columns_ddl}\n"
        f");"
    )


def _get_jdbc_connection(spark: SparkSession, jdbc_url: str, properties: dict[str, str]):
    """Open a raw JDBC connection via PySpark's JVM gateway.

    Args:
        spark: Active Spark session.
        jdbc_url: JDBC connection URL.
        properties: JDBC properties must include ``user`` and ``password``.

    Returns:
        A ``java.sql.Connection`` instance.
    """
    jvm = spark._jvm
    DriverManager = jvm.java.sql.DriverManager
    return DriverManager.getConnection(jdbc_url, properties["user"], properties["password"])


def _execute_jdbc(spark: SparkSession, ddl: str, jdbc_url: str, properties: dict[str, str]) -> None:
    """Execute a DDL statement via direct JDBC connection.

    Args:
        spark: Active Spark session.
        ddl: The DDL statement to execute.
        jdbc_url: JDBC connection URL.
        properties: JDBC connection properties.
    """
    conn = _get_jdbc_connection(spark, jdbc_url, properties)
    try:
        stmt = conn.createStatement()
        stmt.execute(ddl)
    finally:
        conn.close()


def _query_jdbc(spark: SparkSession, sql: str, jdbc_url: str, properties: dict[str, str]):
    """Execute a SELECT query and return the first column of the first row.

    Args:
        spark: Active Spark session.
        sql: The SELECT query to execute.
        jdbc_url: JDBC connection URL.
        properties: JDBC connection properties.

    Returns:
        The value of the first column in the first result row, or ``None``
        if the query returns no rows.
    """
    conn = _get_jdbc_connection(spark, jdbc_url, properties)
    try:
        stmt = conn.createStatement()
        rs = stmt.executeQuery(sql)
        if rs.next():
            return rs.getObject(1)
        return None
    finally:
        conn.close()


def _schema_has_tables(spark: SparkSession, schema: str, jdbc_url: str, properties: dict[str, str]) -> bool:
    """Check whether a PostgreSQL schema contains any tables.

    Args:
        spark: Active Spark session.
        schema: Schema name to check.
        jdbc_url: JDBC connection URL.
        properties: JDBC connection properties.

    Returns:
        ``True`` if the schema has at least one table, ``False`` otherwise.
    """
    sql = (
        f"SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_schema = '{schema}'"
    )
    count = _query_jdbc(spark, sql, jdbc_url, properties)
    return count is not None and count > 0


def create_schema(spark: SparkSession, schema: str, jdbc_url: str, properties: dict[str, str]) -> None:
    """Create a PostgreSQL schema if it does not already exist.

    Args:
        spark: Active Spark session.
        schema: Schema name to create.
        jdbc_url: JDBC connection URL.
        properties: JDBC connection properties.
    """
    ddl = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    _execute_jdbc(spark, ddl, jdbc_url, properties)


def execute_ddl(spark: SparkSession, ddl: str, jdbc_url: str, properties: dict[str, str]) -> None:
    """Execute an arbitrary DDL statement against the database.

    Args:
        spark: Active Spark session.
        ddl: The DDL statement to execute.
        jdbc_url: JDBC connection URL.
        properties: JDBC connection properties.
    """
    _execute_jdbc(spark, ddl, jdbc_url, properties)


def drop_table(spark: SparkSession, table_name: str, schema: str, jdbc_url: str, properties: dict[str, str]) -> None:
    """Drop a table if it exists.

    Args:
        spark: Active Spark session.
        table_name: Table name to drop (without schema prefix).
        schema: PostgreSQL schema name.
        jdbc_url: JDBC connection URL.
        properties: JDBC connection properties.
    """
    ddl = f"DROP TABLE IF EXISTS {schema}.{table_name};"
    _execute_jdbc(spark, ddl, jdbc_url, properties)


def drop_schema(spark: SparkSession, schema: str, jdbc_url: str, properties: dict[str, str]) -> None:
    """Drop a schema only if it exists and contains no tables.

    Args:
        spark: Active Spark session.
        schema: Schema name to drop.
        jdbc_url: JDBC connection URL.
        properties: JDBC connection properties.
    """
    if not _schema_has_tables(spark, schema, jdbc_url, properties):
        ddl = f"DROP SCHEMA IF EXISTS {schema};"
        _execute_jdbc(spark, ddl, jdbc_url, properties)
