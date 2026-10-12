import pytest
from pydantic import ValidationError

from model.config import supabase_config
from model.config.supabase_config import SupabaseConnectionConfig

VALID_CONFIG = {
    "username": "user",
    "password": "pass",
    "host": "db.example.com",
    "port": 5432,
    "database": "postgres",
}

VALID_ENV = {
    "SUPABASE_DB_USER": "user",
    "SUPABASE_DB_PASSWORD": "pass",
    "SUPABASE_DB_HOST": "db.example.com",
    "SUPABASE_DB_PORT": "5432",
    "SUPABASE_DB_NAME": "postgres",
}


@pytest.fixture(autouse=True)
def isolated_dotenv(monkeypatch):
    """Redirect DOTENV_PATH to a nonexistent file so a real .env cannot leak into tests."""
    monkeypatch.setattr(supabase_config, "DOTENV_PATH", "/nonexistent/.env")


@pytest.fixture
def env_config(monkeypatch):
    for name, value in VALID_ENV.items():
        monkeypatch.setenv(name, value)


class TestSupabaseConnectionConfig:
    def test_valid(self):
        cfg = SupabaseConnectionConfig(**VALID_CONFIG)
        assert cfg.username == "user"
        assert cfg.password == "pass"
        assert cfg.host == "db.example.com"
        assert cfg.port == 5432
        assert cfg.database == "postgres"

    def test_frozen(self):
        cfg = SupabaseConnectionConfig(**VALID_CONFIG)
        with pytest.raises(ValidationError):
            cfg.host = "other.example.com"

    def test_from_env(self, env_config):
        cfg = SupabaseConnectionConfig.from_env()
        assert cfg.username == "user"
        assert cfg.password == "pass"
        assert cfg.host == "db.example.com"
        assert cfg.port == 5432
        assert cfg.database == "postgres"

    def test_from_env_missing_all_vars(self, monkeypatch):
        for name in VALID_ENV:
            monkeypatch.delenv(name, raising=False)
        with pytest.raises(ValueError, match="Missing Supabase credentials") as exc_info:
            SupabaseConnectionConfig.from_env()
        for name in VALID_ENV:
            assert name in str(exc_info.value)

    def test_from_env_missing_single_var(self, env_config, monkeypatch):
        monkeypatch.delenv("SUPABASE_DB_PASSWORD")
        with pytest.raises(ValueError, match=r"Missing Supabase credentials: SUPABASE_DB_PASSWORD\b"):
            SupabaseConnectionConfig.from_env()

    def test_from_env_empty_value_counts_as_missing(self, env_config, monkeypatch):
        monkeypatch.setenv("SUPABASE_DB_PASSWORD", "")
        with pytest.raises(ValueError, match=r"Missing Supabase credentials: SUPABASE_DB_PASSWORD\b"):
            SupabaseConnectionConfig.from_env()

    def test_from_env_invalid_port(self, env_config, monkeypatch):
        monkeypatch.setenv("SUPABASE_DB_PORT", "not-a-number")
        with pytest.raises(ValueError, match="SUPABASE_DB_PORT must be an integer"):
            SupabaseConnectionConfig.from_env()

    def test_from_env_loads_dotenv_file(self, monkeypatch, tmp_path):
        dotenv_file = tmp_path / ".env"
        dotenv_file.write_text("\n".join(f"{name}={value}" for name, value in VALID_ENV.items()) + "\n")
        monkeypatch.setattr(supabase_config, "DOTENV_PATH", str(dotenv_file))
        for name in VALID_ENV:
            monkeypatch.delenv(name, raising=False)
        cfg = SupabaseConnectionConfig.from_env()
        assert cfg.host == "db.example.com"
        assert cfg.port == 5432

    def test_from_env_environment_overrides_dotenv(self, monkeypatch, tmp_path):
        dotenv_file = tmp_path / ".env"
        dotenv_file.write_text("SUPABASE_DB_HOST=from-file.example.com\n")
        monkeypatch.setattr(supabase_config, "DOTENV_PATH", str(dotenv_file))
        for name, value in VALID_ENV.items():
            monkeypatch.setenv(name, value)
        cfg = SupabaseConnectionConfig.from_env()
        assert cfg.host == "db.example.com"

    def test_get_jdbc_url(self):
        cfg = SupabaseConnectionConfig(**VALID_CONFIG)
        assert cfg.get_jdbc_url() == "jdbc:postgresql://db.example.com:5432/postgres"

    def test_get_jdbc_properties(self):
        cfg = SupabaseConnectionConfig(**VALID_CONFIG)
        props = cfg.get_jdbc_properties("org.postgresql.Driver")
        assert props == {
            "user": "user",
            "password": "pass",
            "driver": "org.postgresql.Driver",
            "prepareThreshold": "0",
        }
