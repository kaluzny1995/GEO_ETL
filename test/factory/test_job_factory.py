from unittest.mock import MagicMock

import pytest

from factory import JobFactory
from model.enums import JobType


@pytest.fixture(autouse=True)
def clean_registry():
    JobFactory.clear()
    yield
    JobFactory.clear()


class TestJobFactoryRegister:
    def test_register_and_create(self):
        mock_module = MagicMock()
        JobFactory.register(JobType.CREATE, mock_module)
        result = JobFactory.create(JobType.CREATE)
        assert result is mock_module

    def test_create_unknown_raises(self):
        with pytest.raises(ValueError, match="No job registered"):
            JobFactory.create(JobType.CREATE)

    def test_clear_empties_registry(self):
        JobFactory.register(JobType.CREATE, MagicMock())
        JobFactory.clear()
        with pytest.raises(ValueError):
            JobFactory.create(JobType.CREATE)

    def test_register_overwrites_existing(self):
        first = MagicMock()
        second = MagicMock()
        JobFactory.register(JobType.CREATE, first)
        JobFactory.register(JobType.CREATE, second)
        assert JobFactory.create(JobType.CREATE) is second
