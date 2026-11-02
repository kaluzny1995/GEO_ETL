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

VALID_CASES = [
    (SparkConfig, VALID_CONFIG["spark"], {"master": "spark://localhost:7078", "app_name_prefix": "geo_etl"}),
    (DatabaseConfig, VALID_CONFIG["database"], {"db_schema": "geo", "jdbc_driver": "org.postgresql.Driver"}),
    (LoggingConfig, VALID_CONFIG["logging"], {"directory": "logs", "level": "INFO"}),
    (DataConfig, VALID_CONFIG["data"], {"directory": "../GEO_Download/data"}),
    (
        AppConfig,
        VALID_CONFIG,
        {
            "spark.master": "spark://localhost:7078",
            "database.db_schema": "geo",
            "logging.directory": "logs",
            "data.directory": "../GEO_Download/data",
        },
    ),
]

FROZEN_CASES = [
    (SparkConfig, VALID_CONFIG["spark"], "master", "spark://other:7077"),
    (DatabaseConfig, VALID_CONFIG["database"], "db_schema", "other"),
    (LoggingConfig, VALID_CONFIG["logging"], "directory", "other"),
    (DataConfig, VALID_CONFIG["data"], "directory", "other"),
    (AppConfig, VALID_CONFIG, "spark", SparkConfig(master="spark://other:7077")),
]


class TestConfigModels:
    @pytest.mark.parametrize("config_cls, values, expected", VALID_CASES)
    def test_valid(self, config_cls, values, expected):
        # Act
        cfg = config_cls(**values)

        # Assert
        for path, value in expected.items():
            obj = cfg
            for part in path.split("."):
                obj = getattr(obj, part)
            assert obj == value

    @pytest.mark.parametrize("config_cls, values, attr, new_value", FROZEN_CASES)
    def test_frozen(self, config_cls, values, attr, new_value):
        # Arrange
        cfg = config_cls(**values)

        # Assert
        with pytest.raises(ValidationError):
            setattr(cfg, attr, new_value)


class TestSparkConfig:
    def test_default_app_name_prefix(self):
        cfg = SparkConfig(master="spark://localhost:7078")
        assert cfg.app_name_prefix == "geo_etl"

    def test_default_resource_caps(self):
        cfg = SparkConfig(master="spark://localhost:7078")
        assert cfg.driver_memory == "1g"
        assert cfg.executor_memory == "1g"
        assert cfg.executor_cores == 1
        assert cfg.shuffle_partitions == 8
        assert cfg.max_result_size == "256m"

    def test_session_options(self):
        cfg = SparkConfig(
            master="spark://localhost:7078",
            driver_memory="2g",
            executor_memory="3g",
            executor_cores=2,
            shuffle_partitions=16,
            max_result_size="512m",
        )
        assert cfg.session_options == {
            "spark.driver.memory": "2g",
            "spark.executor.memory": "3g",
            "spark.executor.cores": "2",
            "spark.sql.shuffle.partitions": "16",
            "spark.driver.maxResultSize": "512m",
        }


class TestLoggingConfig:
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
    def test_default_values(self):
        cfg = DataConfig()
        assert cfg.directory == "../GEO_Download/data"


class TestAppConfig:
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

    def test_from_json_spark_master_env_override(self, monkeypatch, tmp_path):
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(VALID_CONFIG))
        monkeypatch.setenv("SPARK_MASTER_URL", "spark://geo-spark-master:7078")

        cfg = AppConfig.from_json(str(config_path))

        assert cfg.spark.master == "spark://geo-spark-master:7078"

    def test_from_json_spark_resource_env_overrides(self, monkeypatch, tmp_path):
        config_path = tmp_path / "config.json"
        config_path.write_text(json.dumps(VALID_CONFIG))
        monkeypatch.setenv("SPARK_DRIVER_MEMORY", "512m")
        monkeypatch.setenv("SPARK_EXECUTOR_MEMORY", "2g")
        monkeypatch.setenv("SPARK_EXECUTOR_CORES", "2")
        monkeypatch.setenv("SPARK_SHUFFLE_PARTITIONS", "4")
        monkeypatch.setenv("SPARK_MAX_RESULT_SIZE", "128m")

        cfg = AppConfig.from_json(str(config_path))

        assert cfg.spark.driver_memory == "512m"
        assert cfg.spark.executor_memory == "2g"
        assert cfg.spark.executor_cores == 2
        assert cfg.spark.shuffle_partitions == 4
        assert cfg.spark.max_result_size == "128m"

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

    def test_from_json_without_path_uses_project_config(self):
        cfg = AppConfig.from_json()
        assert cfg.spark.master == "spark://localhost:7078"
        assert cfg.logging.directory == "logs"
        assert cfg.database.db_schema == "geo"
