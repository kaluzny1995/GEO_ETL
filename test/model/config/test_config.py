import json
import logging
import os
import tempfile

import pytest
from pydantic import ValidationError

from model.config.app_config import (
    AppConfig,
    DatabaseConfig,
    DataConfig,
    LoggingConfig,
    SparkConfig,
)

VALID_CONFIG = {
    "spark": {"master": "spark://localhost:7078", "app_name_prefix": "geo_etl"},
    "database": {"db_schema": "geo", "jdbc_driver": "org.postgresql.Driver"},
    "logging": {"directory": "logs", "level": "INFO"},
    "data": {"directory": "../GEO_Download/data"},
}


class TestSparkConfig:
    def test_valid(self):
        cfg = SparkConfig(**VALID_CONFIG["spark"])
        assert cfg.master == "spark://localhost:7078"
        assert cfg.app_name_prefix == "geo_etl"

    def test_frozen(self):
        cfg = SparkConfig(master="spark://localhost:7078")
        with pytest.raises(ValidationError):
            cfg.master = "spark://other:7077"

    def test_default_app_name_prefix(self):
        cfg = SparkConfig(master="spark://localhost:7078")
        assert cfg.app_name_prefix == "geo_etl"


class TestDatabaseConfig:
    def test_valid(self):
        cfg = DatabaseConfig(**VALID_CONFIG["database"])
        assert cfg.db_schema == "geo"
        assert cfg.jdbc_driver == "org.postgresql.Driver"

    def test_frozen(self):
        cfg = DatabaseConfig(db_schema="geo", jdbc_driver="org.postgresql.Driver")
        with pytest.raises(ValidationError):
            cfg.db_schema = "other"


class TestLoggingConfig:
    def test_valid(self):
        cfg = LoggingConfig(**VALID_CONFIG["logging"])
        assert cfg.directory == "logs"
        assert cfg.level == "INFO"

    def test_frozen(self):
        cfg = LoggingConfig(directory="logs", level="INFO")
        with pytest.raises(ValidationError):
            cfg.directory = "other"

    def test_level_int(self):
        cfg = LoggingConfig(level="INFO")
        assert cfg.level_int == logging.INFO

    def test_level_int_debug(self):
        cfg = LoggingConfig(level="DEBUG")
        assert cfg.level_int == logging.DEBUG

    def test_level_int_case_insensitive(self):
        cfg = LoggingConfig(level="info")
        assert cfg.level_int == logging.INFO

    def test_default_values(self):
        cfg = LoggingConfig()
        assert cfg.directory == "logs"
        assert cfg.level == "INFO"


class TestDataConfig:
    def test_valid(self):
        cfg = DataConfig(**VALID_CONFIG["data"])
        assert cfg.directory == "../GEO_Download/data"

    def test_frozen(self):
        cfg = DataConfig(directory="data")
        with pytest.raises(ValidationError):
            cfg.directory = "other"

    def test_default_values(self):
        cfg = DataConfig()
        assert cfg.directory == "../GEO_Download/data"


class TestAppConfig:
    def test_valid(self):
        cfg = AppConfig(**VALID_CONFIG)
        assert cfg.spark.master == "spark://localhost:7078"
        assert cfg.database.db_schema == "geo"
        assert cfg.logging.directory == "logs"
        assert cfg.data.directory == "../GEO_Download/data"

    def test_frozen(self):
        cfg = AppConfig(**VALID_CONFIG)
        with pytest.raises(ValidationError):
            cfg.spark = SparkConfig(master="spark://other:7077")

    def test_from_json(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(VALID_CONFIG, f)
            f.flush()
            cfg = AppConfig.from_json(f.name)
        os.unlink(f.name)
        assert cfg.spark.master == "spark://localhost:7078"
        assert cfg.database.db_schema == "geo"
        assert cfg.logging.directory == "logs"
        assert cfg.data.directory == "../GEO_Download/data"

    def test_from_json_missing_file(self):
        with pytest.raises(FileNotFoundError):
            AppConfig.from_json("/nonexistent/config.json")

    def test_from_json_invalid_data(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"spark": {}}, f)
            f.flush()
            with pytest.raises(ValidationError):
                AppConfig.from_json(f.name)
        os.unlink(f.name)

    def test_optional_sections_default(self):
        minimal = {
            "spark": {"master": "spark://localhost:7078"},
            "database": {"db_schema": "geo", "jdbc_driver": "org.postgresql.Driver"},
        }
        cfg = AppConfig(**minimal)
        assert cfg.logging.directory == "logs"
        assert cfg.logging.level == "INFO"
        assert cfg.data.directory == "../GEO_Download/data"
