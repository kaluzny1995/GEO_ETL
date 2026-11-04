"""Supabase PostgreSQL connection configuration."""

import os

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(os.path.dirname(SCRIPT_DIR))
DOTENV_PATH = os.path.join(PROJECT_DIR, ".env")

ENV_USER = "SUPABASE_DB_USER"
ENV_PASSWORD = "SUPABASE_DB_PASSWORD"
ENV_HOST = "SUPABASE_DB_HOST"
ENV_PORT = "SUPABASE_DB_PORT"
ENV_NAME = "SUPABASE_DB_NAME"
REQUIRED_ENV_VARS = (ENV_USER, ENV_PASSWORD, ENV_HOST, ENV_PORT, ENV_NAME)


class SupabaseConnectionConfig(BaseModel):
    """Supabase PostgreSQL connection credentials.

    Attributes:
        username: Database username (e.g. ``"postgres.<project-ref>"``).
        password: Database password.
        host: Pooler hostname.
        port: Database port (typically ``6543``).
        database: Database name (typically ``"postgres"``).
    """

    model_config = ConfigDict(frozen=True)

    username: str
    password: str
    host: str
    port: int
    database: str

    @staticmethod
    def from_env() -> "SupabaseConnectionConfig":
        """Load credentials from environment variables.

        Reads ``SUPABASE_DB_USER``, ``SUPABASE_DB_PASSWORD``, ``SUPABASE_DB_HOST``,
        ``SUPABASE_DB_PORT`` and ``SUPABASE_DB_NAME``. A ``.env`` file in the
        project root is loaded first; variables already present in the
        environment take precedence over the file.

        Returns:
            A frozen :class:`SupabaseConnectionConfig` instance.

        Raises:
            ValueError: If any required variable is missing or empty, or if
                ``SUPABASE_DB_PORT`` is not an integer.
        """
        load_dotenv(DOTENV_PATH)
        missing = [name for name in REQUIRED_ENV_VARS if not os.environ.get(name)]
        if missing:
            raise ValueError(
                f"Missing Supabase credentials: {', '.join(missing)}. "
                "Set them as environment variables or in a .env file in the project root "
                "(see .env.example and docs/configuration/supabase-credentials.md)."
            )
        raw_port = os.environ[ENV_PORT]
        try:
            port = int(raw_port)
        except ValueError as exc:
            raise ValueError(f"{ENV_PORT} must be an integer, got {raw_port!r}") from exc
        return SupabaseConnectionConfig(
            username=os.environ[ENV_USER],
            password=os.environ[ENV_PASSWORD],
            host=os.environ[ENV_HOST],
            port=port,
            database=os.environ[ENV_NAME],
        )

    def get_jdbc_url(self) -> str:
        """Build the JDBC connection URL.

        Returns:
            A JDBC URL string
            (e.g. ``"jdbc:postgresql://host:6543/postgres"``).
        """
        return f"jdbc:postgresql://{self.host}:{self.port}/{self.database}"

    def get_jdbc_properties(self, jdbc_driver: str) -> dict[str, str]:
        """Build the JDBC connection properties dictionary.

        ``prepareThreshold=0`` turns off server-side prepared statements: the
        Supabase transaction pooler reuses one server connection for many
        clients, so named statements (``S_1`` …) can be prepared twice and fail
        with ``prepared statement "S_1" already exists``.

        Args:
            jdbc_driver: Fully qualified JDBC driver class name.

        Returns:
            A dictionary with ``user``, ``password``, ``driver`` and
            ``prepareThreshold`` keys.
        """
        return {
            "user": self.username,
            "password": self.password,
            "driver": jdbc_driver,
            "prepareThreshold": "0",
        }
