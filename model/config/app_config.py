import json
import os
from typing import Optional

from pydantic import BaseModel, ConfigDict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(os.path.dirname(SCRIPT_DIR))
CONFIG_PATH = os.path.join(PROJECT_DIR, "config.json")


class SparkConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    master: str


class DatabaseConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    db_schema: str
    jdbc_driver: str


class AppConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    spark: SparkConfig
    database: DatabaseConfig

    @staticmethod
    def from_json(path: Optional[str] = None) -> "AppConfig":
        if path is None:
            path = CONFIG_PATH
        with open(path) as f:
            return AppConfig(**json.load(f))
