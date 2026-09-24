"""Supabase PostgreSQL connection configuration."""

import json
import os
from typing import Optional

from pydantic import BaseModel, ConfigDict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(os.path.dirname(SCRIPT_DIR))
SUPABASE_CONFIG_PATH = os.path.join(PROJECT_DIR, "supabase_config.json")


class SupabaseConnectionConfig(BaseModel):
    """Supabase PostgreSQL connection credentials.

    Attributes:
        username: Database username (e.g. ``"postgres.<project-ref>"``).
        password: Database password.
        host: Pooler hostname.
        port: Database port (typically ``5432``).
        database: Database name (typically ``"postgres"``).
    """
    model_config = ConfigDict(frozen=True)

    username: str
    password: str
    host: str
    port: int
    database: str

    @staticmethod
    def from_json(path: Optional[str] = None) -> "SupabaseConnectionConfig":
        """Load credentials from ``supabase_config.json``.

        Args:
            path: Path to the JSON credentials file. Defaults to
                ``supabase_config.json`` in the project root.

        Returns:
            A frozen :class:`SupabaseConnectionConfig` instance.
        """
        if path is None:
            path = SUPABASE_CONFIG_PATH
        with open(path) as f:
            return SupabaseConnectionConfig(**json.load(f))

    def get_jdbc_url(self) -> str:
        """Build the JDBC connection URL.

        Returns:
            A JDBC URL string
            (e.g. ``"jdbc:postgresql://host:5432/postgres"``).
        """
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    def get_jdbc_properties(self, jdbc_driver: str) -> dict[str, str]:
        """Build the JDBC connection properties dictionary.

        Args:
            jdbc_driver: Fully qualified JDBC driver class name.

        Returns:
            A dictionary with ``user``, ``password``, and ``driver`` keys.
        """
        return {
            "user": self.username,
            "password": self.password,
            "driver": jdbc_driver,
        }
