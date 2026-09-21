import json
import os
from typing import Optional

from pydantic import BaseModel, ConfigDict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(os.path.dirname(SCRIPT_DIR))
SUPABASE_CONFIG_PATH = os.path.join(PROJECT_DIR, "supabase_config.json")


class SupabaseConnectionConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    username: str
    password: str
    host: str
    port: int
    database: str

    @staticmethod
    def from_json(path: Optional[str] = None) -> "SupabaseConnectionConfig":
        if path is None:
            path = SUPABASE_CONFIG_PATH
        with open(path) as f:
            return SupabaseConnectionConfig(**json.load(f))

    def get_jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    def get_jdbc_properties(self, jdbc_driver: str) -> dict:
        return {
            "user": self.username,
            "password": self.password,
            "driver": jdbc_driver,
        }
