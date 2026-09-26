"""Application configuration models loaded from ``config.json``."""

import json
import logging
import os

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
    """Spark cluster connection settings.

    Attributes:
        master: Spark master URL (e.g. ``"spark://localhost:7078"``).
        app_name_prefix: Prefix for Spark application names.
    """

    model_config = ConfigDict(frozen=True)

    master: str
    app_name_prefix: str = "geo_etl"


class DatabaseConfig(BaseModel):
    """PostgreSQL database settings.

    Attributes:
        db_schema: Target schema name (e.g. ``"geo"``).
        jdbc_driver: Fully qualified JDBC driver class name.
    """

    model_config = ConfigDict(frozen=True)

    db_schema: str
    jdbc_driver: str


class LoggingConfig(BaseModel):
    """Logging settings.

    Attributes:
        directory: Root directory for log files.
        level: Log level name (``"DEBUG"``, ``"INFO"``, ``"WARNING"``, etc.).
    """

    model_config = ConfigDict(frozen=True)

    directory: str = "logs"
    level: str = "INFO"

    @property
    def level_int(self) -> int:
        """Return the log level as a :mod:`logging` integer constant."""
        return LOG_LEVEL_MAP.get(self.level.upper(), logging.INFO)


class DataConfig(BaseModel):
    """Data source settings.

    Attributes:
        directory: Relative path to the CSV data directory
            (e.g. ``"../GEO_Download/data"``).
    """

    model_config = ConfigDict(frozen=True)

    directory: str = "../GEO_Download/data"


class AppConfig(BaseModel):
    """Top-level application configuration loaded from ``config.json``.

    Attributes:
        spark: Spark cluster settings.
        database: Database connection settings.
        logging: Logging configuration.
        data: Data source paths.
    """

    model_config = ConfigDict(frozen=True)

    spark: SparkConfig
    database: DatabaseConfig
    logging: LoggingConfig = LoggingConfig()
    data: DataConfig = DataConfig()

    @staticmethod
    def from_json(path: str | None = None) -> "AppConfig":
        """Load configuration from a JSON file.

        Args:
            path: Path to the JSON config file. Defaults to ``config.json``
                in the project root.

        Returns:
            A frozen :class:`AppConfig` instance.
        """
        if path is None:
            path = CONFIG_PATH
        with open(path) as f:
            return AppConfig(**json.load(f))
