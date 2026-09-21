import types
from typing import Union, get_args, get_origin

from pydantic import BaseModel

from pyspark.sql import SparkSession


def _resolve_type(annotation: type) -> type:
    """Extract the base type from Optional/Union annotations (e.g. str | None -> str)."""
    origin = get_origin(annotation)
    if origin is Union or isinstance(annotation, types.UnionType):
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0]
    return annotation


def _pg_type(annotation: type) -> str:
    """Map Python type to PostgreSQL type string."""
    resolved = _resolve_type(annotation)
    mapping = {
        str: "TEXT",
    }
    result = mapping.get(resolved)
    if result is None:
        raise TypeError(f"Unsupported type '{resolved.__name__}' for PostgreSQL DDL")
    return result


def pydantic_to_ddl(model: type[BaseModel], table_name: str, schema: str = "raw") -> str:
    """Convert Pydantic model fields to CREATE TABLE DDL statement."""
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


def _execute_jdbc(spark: SparkSession, ddl: str, jdbc_url: str, properties: dict) -> None:
    """Execute DDL via direct JDBC connection (Spark's read.jdbc only supports SELECT)."""
    jvm = spark._jvm
    DriverManager = jvm.java.sql.DriverManager
    conn = DriverManager.getConnection(jdbc_url, properties["user"], properties["password"])
    try:
        stmt = conn.createStatement()
        stmt.execute(ddl)
    finally:
        conn.close()


def create_schema(spark: SparkSession, schema: str, jdbc_url: str, properties: dict) -> None:
    """Create PostgreSQL schema if it doesn't exist."""
    ddl = f"CREATE SCHEMA IF NOT EXISTS {schema};"
    _execute_jdbc(spark, ddl, jdbc_url, properties)


def execute_ddl(spark: SparkSession, ddl: str, jdbc_url: str, properties: dict) -> None:
    """Execute arbitrary DDL statement against the database."""
    _execute_jdbc(spark, ddl, jdbc_url, properties)
