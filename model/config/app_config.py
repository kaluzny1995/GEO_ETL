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
    """Spark cluster connection and resource settings.

    Attributes:
        master: Spark master URL (e.g. ``"spark://localhost:7078"``).
        app_name_prefix: Prefix for Spark application names.
        driver_memory: Heap cap for the PySpark driver JVM (runs inside the Airflow
            scheduler container in Docker).
        executor_memory: Heap cap for each Spark executor.
        executor_cores: Cores allocated to each executor.
        shuffle_partitions: Default number of shuffle partitions; a low value keeps the
            small GIS datasets from spawning hundreds of tiny tasks.
        max_result_size: Maximum size of a result collected to the driver.
    """

    model_config = ConfigDict(frozen=True)

    master: str
    app_name_prefix: str = "geo_etl"
    driver_memory: str = "1g"
    executor_memory: str = "1g"
    executor_cores: int = 1
    shuffle_partitions: int = 8
    max_result_size: str = "256m"

    @property
    def session_options(self) -> dict[str, str]:
        """Return the Spark configuration keys derived from these settings."""
        return {
            "spark.driver.memory": self.driver_memory,
            "spark.executor.memory": self.executor_memory,
            "spark.executor.cores": str(self.executor_cores),
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.driver.maxResultSize": self.max_result_size,
        }


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

        Environment variables override the matching ``spark`` settings so the same
        ``config.json`` works locally and inside Docker Compose:

        ``SPARK_MASTER_URL``, ``SPARK_DRIVER_MEMORY``, ``SPARK_EXECUTOR_MEMORY``,
        ``SPARK_EXECUTOR_CORES``, ``SPARK_SHUFFLE_PARTITIONS`` and
        ``SPARK_MAX_RESULT_SIZE``.

        Args:
            path: Path to the JSON config file. Defaults to ``config.json``
                in the project root.

        Returns:
            A frozen :class:`AppConfig` instance.
        """
        if path is None:
            path = CONFIG_PATH
        with open(path) as f:
            data = json.load(f)
        spark = data.setdefault("spark", {})
        env_overrides = {
            "SPARK_MASTER_URL": "master",
            "SPARK_DRIVER_MEMORY": "driver_memory",
            "SPARK_EXECUTOR_MEMORY": "executor_memory",
            "SPARK_EXECUTOR_CORES": "executor_cores",
            "SPARK_SHUFFLE_PARTITIONS": "shuffle_partitions",
            "SPARK_MAX_RESULT_SIZE": "max_result_size",
        }
        for env_name, field in env_overrides.items():
            value = os.environ.get(env_name)
            if value:
                spark[field] = value
        return AppConfig(**data)
