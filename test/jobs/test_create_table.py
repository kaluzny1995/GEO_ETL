from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from factory import TableFactory
from jobs.create_table import run
from test.conftest import mock_app_config, mock_db_config


class DummyModel(BaseModel):
    name: str


@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.read.jdbc.return_value = MagicMock()
    return spark


class TestCreateTableRun:
    @patch("jobs.create_table.TableFactory.get", return_value=DummyModel)
    @patch("jobs.create_table.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
    @patch("jobs.create_table.AppConfig.from_json", return_value=mock_app_config())
    @patch("jobs.create_table.execute_ddl")
    @patch("jobs.create_table.create_schema")
    def test_run_returns_success_message(
        self, mock_create_schema, mock_execute_ddl, mock_app, mock_db, mock_get, mock_spark
    ):
        result = run(mock_spark, "avalanche_org")
        assert "avalanche_org" in result
        assert "created" in result

    @patch("jobs.create_table.TableFactory.get", return_value=DummyModel)
    @patch("jobs.create_table.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
    @patch("jobs.create_table.AppConfig.from_json", return_value=mock_app_config())
    @patch("jobs.create_table.execute_ddl")
    @patch("jobs.create_table.create_schema")
    def test_run_creates_geo_schema(
        self, mock_create_schema, mock_execute_ddl, mock_app, mock_db, mock_get, mock_spark
    ):
        run(mock_spark, "avalanche_org")
        mock_create_schema.assert_called_once()
        call_args = mock_create_schema.call_args
        assert call_args[0][1] == "geo"

    @patch("jobs.create_table.TableFactory.get", return_value=DummyModel)
    @patch("jobs.create_table.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
    @patch("jobs.create_table.AppConfig.from_json", return_value=mock_app_config())
    @patch("jobs.create_table.execute_ddl")
    @patch("jobs.create_table.create_schema")
    def test_run_executes_ddl(self, mock_create_schema, mock_execute_ddl, mock_app, mock_db, mock_get, mock_spark):
        run(mock_spark, "avalanche_org")
        mock_execute_ddl.assert_called_once()
        ddl = mock_execute_ddl.call_args[0][1]
        assert "CREATE TABLE IF NOT EXISTS geo.avalanche_org" in ddl

    def test_unknown_table_raises(self, mock_spark):
        with pytest.raises(ValueError, match="Unknown table"):
            run(mock_spark, "nonexistent_table")

    def test_table_registry_contains_avalanche_org(self):
        # Re-register in case another test module cleared the registry
        from model import AvalancheOrg

        TableFactory.register("avalanche_org", AvalancheOrg)
        assert "avalanche_org" in TableFactory.tables()

    @patch("jobs.create_table.TableFactory.get", return_value=DummyModel)
    @patch("jobs.create_table.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
    @patch("jobs.create_table.AppConfig.from_json", return_value=mock_app_config())
    @patch("jobs.create_table.execute_ddl")
    @patch("jobs.create_table.create_schema")
    def test_run_logs_steps(self, mock_create_schema, mock_execute_ddl, mock_app, mock_db, mock_get, mock_spark):
        mock_logger = MagicMock()
        run(mock_spark, "avalanche_org", logger=mock_logger)
        mock_logger.info.assert_any_call("Creating schema 'geo' if not exists...")
        mock_logger.info.assert_any_call("Schema 'geo' ready")
        mock_logger.info.assert_any_call("Generated DDL for geo.avalanche_org")
        mock_logger.info.assert_any_call("Executing DDL against Supabase...")
        mock_logger.info.assert_any_call("DDL executed successfully")
