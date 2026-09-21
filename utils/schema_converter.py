import datetime as dt
import enum
import types
from typing import Optional, Union, get_args, get_origin

from pydantic import BaseModel
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

_PYSPARK_TYPE_MAP: dict[type, type] = {
    str: StringType,
    int: LongType,
    float: DoubleType,
    bool: BooleanType,
    dt.datetime: TimestampType,
    dt.date: DateType,
}


def _resolve_type(annotation: type) -> tuple[type, bool]:
    """Extract base type and nullable flag from Optional/Union annotations."""
    origin = get_origin(annotation)

    if origin is Union or isinstance(annotation, types.UnionType):
        args = [a for a in get_args(annotation) if a is not type(None)]
        if len(args) == 1:
            return args[0], True
        return annotation, False

    return annotation, False


def pydantic_to_spark_schema(model: type[BaseModel]) -> StructType:
    """Convert Pydantic model fields to PySpark StructType schema."""
    fields = []
    for name, field_info in model.model_fields.items():
        annotation = field_info.annotation
        python_type, nullable = _resolve_type(annotation)

        # Enums are treated as strings in Spark
        if isinstance(python_type, type) and issubclass(python_type, enum.Enum):
            spark_type = StringType()
        else:
            spark_type_class = _PYSPARK_TYPE_MAP.get(python_type)
            if spark_type_class is None:
                raise TypeError(
                    f"Unsupported type '{python_type.__name__}' for field '{name}'"
                )
            spark_type = spark_type_class()

        fields.append(StructField(name, spark_type, nullable))

    return StructType(fields)
