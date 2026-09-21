from unittest.mock import MagicMock, patch

import pytest
from pydantic import BaseModel

from jobs.create_table import run
from factory import TableFactory


class DummyModel(BaseModel):
    name: str


def _mock_app_config():
    cfg = MagicMock()
    cfg.database.db_schema = "geo"
    cfg.database.jdbc_driver = "org.postgresql.Driver"
    return cfg


def _mock_db_config():
    cfg = MagicMock()
    cfg.get_jdbc_url.return_value = "jdbc:postgresql://db.example.com:5432/postgres"
    cfg.get_jdbc_properties.return_value = {
        "user": "user",
        "password": "pass",
        "driver": "org.postgresql.Driver",
    }
    return cfg


@pytest.fixture
def mock_spark():
    spark = MagicMock()
    spark.read.jdbc.return_value = MagicMock()
    return spark


class TestCreateTableRun:
    @patch("jobs.create_table.TableFactory.get", return_value=DummyModel)
    @patch("jobs.create_table.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
    @patch("jobs.create_table.AppConfig.from_json", return_value=_mock_app_config())
    @patch("jobs.create_table.execute_ddl")
    @patch("jobs.create_table.create_schema")
    def test_run_returns_success_message(self, mock_create_schema, mock_execute_ddl, mock_app, mock_db, mock_get, mock_spark):
        result = run(mock_spark, "avalanche_org_raw")
        assert "avalanche_org_raw" in result
        assert "created" in result

    @patch("jobs.create_table.TableFactory.get", return_value=DummyModel)
    @patch("jobs.create_table.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
    @patch("jobs.create_table.AppConfig.from_json", return_value=_mock_app_config())
    @patch("jobs.create_table.execute_ddl")
    @patch("jobs.create_table.create_schema")
    def test_run_creates_geo_schema(self, mock_create_schema, mock_execute_ddl, mock_app, mock_db, mock_get, mock_spark):
        run(mock_spark, "avalanche_org_raw")
        mock_create_schema.assert_called_once()
        call_args = mock_create_schema.call_args
        assert call_args[0][1] == "geo"

    @patch("jobs.create_table.TableFactory.get", return_value=DummyModel)
    @patch("jobs.create_table.SupabaseConnectionConfig.from_json", return_value=_mock_db_config())
    @patch("jobs.create_table.AppConfig.from_json", return_value=_mock_app_config())
    @patch("jobs.create_table.execute_ddl")
    @patch("jobs.create_table.create_schema")
    def test_run_executes_ddl(self, mock_create_schema, mock_execute_ddl, mock_app, mock_db, mock_get, mock_spark):
        run(mock_spark, "avalanche_org_raw")
        mock_execute_ddl.assert_called_once()
        ddl = mock_execute_ddl.call_args[0][1]
        assert "CREATE TABLE IF NOT EXISTS geo.avalanche_org_raw" in ddl

    def test_unknown_table_raises(self, mock_spark):
        with pytest.raises(ValueError, match="Unknown table"):
            run(mock_spark, "nonexistent_table")

    def test_table_registry_contains_avalanche_org_raw(self):
        # Re-register in case another test module cleared the registry
        from model.raw import AvalancheOrgRaw
        TableFactory.register("avalanche_org_raw", AvalancheOrgRaw)
        assert "avalanche_org_raw" in TableFactory.tables()
