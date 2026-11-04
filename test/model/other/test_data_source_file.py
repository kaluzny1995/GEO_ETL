"""Tests for the published CSV metadata models."""

from pyspark.sql.types import DoubleType, LongType, StringType

from model.other.data_source_file import DataSourceFile, DataSourceSource
from utils.schema_converter import pydantic_to_spark_schema


def test_file_schema_matches_the_published_columns() -> None:
    """The Spark schema carries every file-metadata column with its expected type."""
    schema = pydantic_to_spark_schema(DataSourceFile)
    fields = {field.name: field.dataType for field in schema.fields}

    assert fields == {
        "source_name": StringType(),
        "file_name": StringType(),
        "snapshot_date": StringType(),
        "created_at": StringType(),
        "records": LongType(),
        "size_kb": DoubleType(),
        "pipeline_start_date": StringType(),
    }


def test_source_schema_matches_the_published_columns() -> None:
    """The source registry carries the source name, start date and refresh time."""
    schema = pydantic_to_spark_schema(DataSourceSource)
    fields = {field.name: field.dataType for field in schema.fields}

    assert fields == {
        "source_name": StringType(),
        "pipeline_start_date": StringType(),
        "updated_at": StringType(),
    }
