import datetime as dt
import enum

import pytest
from pydantic import BaseModel
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructType,
    TimestampType,
)

from utils.schema_converter import pydantic_to_spark_schema


class Color(enum.Enum):
    RED = "red"
    BLUE = "blue"


class BasicModel(BaseModel):
    name: str
    age: int
    score: float
    active: bool


class OptionalModel(BaseModel):
    name: str
    nickname: str | None = None
    middle_name: str | None = None


class DateTimeModel(BaseModel):
    created_at: dt.datetime
    birth_date: dt.date


class EnumModel(BaseModel):
    color: Color
    label: str


class EmptyModel(BaseModel):
    pass


class MixedModel(BaseModel):
    id: int
    name: str
    value: float
    enabled: bool
    created_at: dt.datetime
    updated_at: dt.datetime | None = None
    color: Color
    tag: str | None = None


def test_basic_types():
    schema = pydantic_to_spark_schema(BasicModel)
    assert isinstance(schema, StructType)
    assert len(schema.fields) == 4

    names = [f.name for f in schema.fields]
    assert names == ["name", "age", "score", "active"]

    types = [f.dataType for f in schema.fields]
    assert types == [StringType(), LongType(), DoubleType(), BooleanType()]

    nullables = [f.nullable for f in schema.fields]
    assert nullables == [False, False, False, False]


def test_optional_types():
    schema = pydantic_to_spark_schema(OptionalModel)
    assert len(schema.fields) == 3

    name_field = schema.fields[0]
    assert name_field.nullable is False

    nickname_field = schema.fields[1]
    assert nickname_field.nullable is True
    assert isinstance(nickname_field.dataType, StringType)

    middle_field = schema.fields[2]
    assert middle_field.nullable is True
    assert isinstance(middle_field.dataType, StringType)


def test_datetime_types():
    schema = pydantic_to_spark_schema(DateTimeModel)
    assert len(schema.fields) == 2

    assert isinstance(schema.fields[0].dataType, TimestampType)
    assert isinstance(schema.fields[1].dataType, DateType)


def test_enum_type():
    schema = pydantic_to_spark_schema(EnumModel)
    assert len(schema.fields) == 2

    color_field = schema.fields[0]
    assert isinstance(color_field.dataType, StringType)
    assert color_field.nullable is False

    label_field = schema.fields[1]
    assert isinstance(label_field.dataType, StringType)


def test_empty_model():
    schema = pydantic_to_spark_schema(EmptyModel)
    assert isinstance(schema, StructType)
    assert len(schema.fields) == 0


def test_mixed_types():
    schema = pydantic_to_spark_schema(MixedModel)
    assert len(schema.fields) == 8

    expected = [
        ("id", LongType(), False),
        ("name", StringType(), False),
        ("value", DoubleType(), False),
        ("enabled", BooleanType(), False),
        ("created_at", TimestampType(), False),
        ("updated_at", TimestampType(), True),
        ("color", StringType(), False),
        ("tag", StringType(), True),
    ]

    for field, (exp_name, exp_type, exp_nullable) in zip(schema.fields, expected, strict=True):
        assert field.name == exp_name
        assert field.dataType == exp_type
        assert field.nullable == exp_nullable


def test_unsupported_type_raises():
    class UnsupportedModel(BaseModel):
        data: bytes

    with pytest.raises(TypeError, match="Unsupported type 'bytes'"):
        pydantic_to_spark_schema(UnsupportedModel)
