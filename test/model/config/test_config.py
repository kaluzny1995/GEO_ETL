import json
import os
import tempfile

import pytest

from model.config.app_config import AppConfig, SparkConfig, DatabaseConfig


VALID_CONFIG = {
    "spark": {"master": "spark://localhost:7078"},
    "database": {"db_schema": "geo", "jdbc_driver": "org.postgresql.Driver"},
}


class TestSparkConfig:
    def test_valid(self):
        cfg = SparkConfig(**VALID_CONFIG["spark"])
        assert cfg.master == "spark://localhost:7078"

    def test_frozen(self):
        cfg = SparkConfig(master="spark://localhost:7078")
        with pytest.raises(Exception):
            cfg.master = "spark://other:7077"


class TestDatabaseConfig:
    def test_valid(self):
        cfg = DatabaseConfig(**VALID_CONFIG["database"])
        assert cfg.db_schema == "geo"
        assert cfg.jdbc_driver == "org.postgresql.Driver"

    def test_frozen(self):
        cfg = DatabaseConfig(db_schema="geo", jdbc_driver="org.postgresql.Driver")
        with pytest.raises(Exception):
            cfg.db_schema = "other"


class TestAppConfig:
    def test_valid(self):
        cfg = AppConfig(**VALID_CONFIG)
        assert cfg.spark.master == "spark://localhost:7078"
        assert cfg.database.db_schema == "geo"

    def test_frozen(self):
        cfg = AppConfig(**VALID_CONFIG)
        with pytest.raises(Exception):
            cfg.spark = SparkConfig(master="spark://other:7077")

    def test_from_json(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(VALID_CONFIG, f)
            f.flush()
            cfg = AppConfig.from_json(f.name)
        os.unlink(f.name)
        assert cfg.spark.master == "spark://localhost:7078"
        assert cfg.database.db_schema == "geo"

    def test_from_json_missing_file(self):
        with pytest.raises(FileNotFoundError):
            AppConfig.from_json("/nonexistent/config.json")

    def test_from_json_invalid_data(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"spark": {}}, f)
            f.flush()
            with pytest.raises(Exception):
                AppConfig.from_json(f.name)
        os.unlink(f.name)
