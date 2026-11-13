import datetime as dt

import pytest
from pydantic import ValidationError
from pyspark.sql.types import BooleanType, DateType, DoubleType, LongType, StringType, TimestampType

from factory import TableFactory
from model.other.prediction_etl import PredictionEtl
from utils.ddl_generator import pydantic_to_ddl
from utils.schema_converter import pydantic_to_spark_schema


def valid_row() -> PredictionEtl:
    return PredictionEtl(
        event_category="avalanche",
        metric_name="size",
        metric_unit="destructive size class",
        file_date=dt.date(2026, 11, 1),
        predicted_value=2.4,
        is_predicted=True,
        model_name="gbt_regressor",
        training_observations=24,
        validation_rmse=0.42,
        predicted_at=dt.datetime(2026, 11, 12, 10, 0, 0),
    )


class TestPredictionEtlValid:
    def test_valid_row(self):
        row = valid_row()
        assert row.predicted_value == 2.4
        assert row.is_predicted is True
        assert row.model_name == "gbt_regressor"

    def test_unit_is_optional(self):
        row = valid_row()
        row = row.model_copy(update={"metric_unit": None})
        assert row.metric_unit is None

    def test_validation_rmse_is_optional(self):
        row = valid_row()
        row = row.model_copy(update={"validation_rmse": None})
        assert row.validation_rmse is None


class TestPredictionEtlMissing:
    def test_required_fields_raise(self):
        with pytest.raises(ValidationError):
            PredictionEtl(event_category="avalanche")

    def test_missing_predicted_value_raises(self):
        with pytest.raises(ValidationError):
            PredictionEtl(
                event_category="avalanche",
                metric_name="size",
                file_date=dt.date(2026, 11, 1),
                is_predicted=True,
                model_name="gbt_regressor",
                training_observations=5,
                predicted_at=dt.datetime(2026, 11, 12),
            )


class TestPredictionEtlSchema:
    def test_spark_schema_field_types(self):
        schema = pydantic_to_spark_schema(PredictionEtl)
        fields = {field.name: field for field in schema.fields}
        assert fields["event_category"].dataType == StringType()
        assert fields["metric_unit"].dataType == StringType()
        assert fields["file_date"].dataType == DateType()
        assert fields["predicted_value"].dataType == DoubleType()
        assert fields["is_predicted"].dataType == BooleanType()
        assert fields["training_observations"].dataType == LongType()
        assert fields["validation_rmse"].dataType == DoubleType()
        assert fields["predicted_at"].dataType == TimestampType()


class TestPredictionEtlDdl:
    def test_ddl_types(self):
        ddl = pydantic_to_ddl(PredictionEtl, "prediction_etl", schema="geo")
        assert "event_category TEXT NOT NULL" in ddl
        assert "metric_unit TEXT NULL" in ddl
        assert "file_date DATE NOT NULL" in ddl
        assert "predicted_value DOUBLE PRECISION NOT NULL" in ddl
        assert "is_predicted BOOLEAN NOT NULL" in ddl
        assert "training_observations INTEGER NOT NULL" in ddl
        assert "predicted_at TIMESTAMP NOT NULL" in ddl
        assert "CREATE TABLE IF NOT EXISTS geo.prediction_etl" in ddl


def test_registered_in_factory():
    assert TableFactory.get("prediction_etl") is PredictionEtl
