import json
import os
import tempfile

import pytest
from pydantic import ValidationError

from model.config.supabase_config import SupabaseConnectionConfig

VALID_CONFIG = {
    "username": "user",
    "password": "pass",
    "host": "db.example.com",
    "port": 5432,
    "database": "postgres",
}


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

    def test_from_json(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(VALID_CONFIG, f)
            f.flush()
            cfg = SupabaseConnectionConfig.from_json(f.name)
        os.unlink(f.name)
        assert cfg.host == "db.example.com"
        assert cfg.port == 5432

    def test_from_json_missing_file(self):
        with pytest.raises(FileNotFoundError):
            SupabaseConnectionConfig.from_json("/nonexistent/config.json")

    def test_from_json_invalid_data(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump({"host": "db.example.com"}, f)
            f.flush()
            with pytest.raises(ValidationError):
                SupabaseConnectionConfig.from_json(f.name)
        os.unlink(f.name)

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
        }
