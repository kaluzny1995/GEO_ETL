import json
import logging
import os
from typing import Optional

from pydantic import BaseModel, ConfigDict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(os.path.dirname(SCRIPT_DIR))
CONFIG_PATH = os.path.join(PROJECT_DIR, "config.json")

LOG_LEVEL_MAP = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
}


class SparkConfig(BaseModel):
    """Spark cluster connection settings."""
    model_config = ConfigDict(frozen=True)

    master: str
    app_name_prefix: str = "geo_etl"


class DatabaseConfig(BaseModel):
    """PostgreSQL database settings."""
    model_config = ConfigDict(frozen=True)

    db_schema: str
    jdbc_driver: str


class LoggingConfig(BaseModel):
    """Logging settings."""
    model_config = ConfigDict(frozen=True)

    directory: str = "logs"
    level: str = "INFO"

    @property
    def level_int(self) -> int:
        return LOG_LEVEL_MAP.get(self.level.upper(), logging.INFO)


class DataConfig(BaseModel):
    """Data source settings."""
    model_config = ConfigDict(frozen=True)

    directory: str = "../GEO_Download/data"


class AppConfig(BaseModel):
    """Application configuration loaded from config.json."""
    model_config = ConfigDict(frozen=True)

    spark: SparkConfig
    database: DatabaseConfig
    logging: LoggingConfig = LoggingConfig()
    data: DataConfig = DataConfig()

    @staticmethod
    def from_json(path: Optional[str] = None) -> "AppConfig":
        """Load config from JSON file."""
        if path is None:
            path = CONFIG_PATH
        with open(path) as f:
            return AppConfig(**json.load(f))
