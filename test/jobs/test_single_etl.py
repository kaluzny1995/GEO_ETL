from unittest.mock import MagicMock, patch

import pytest

from factory import SingleEtlFactory
from model.enums import LoadType
from test.conftest import mock_app_config, mock_db_config
from utils.etl_utils import EtlOutcome


@pytest.fixture(autouse=True)
def _clean_registry():
    SingleEtlFactory.clear()
    yield
    SingleEtlFactory.clear()


@patch("jobs.single_etl.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("jobs.single_etl.AppConfig.from_json", return_value=mock_app_config(logging=True))
def test_single_etl_runs_registered_script(mock_app, mock_db):
    spark = MagicMock()
    script = MagicMock()
    script.run.return_value = EtlOutcome(True, "HISTORICAL: wrote to geo.example_target")
    SingleEtlFactory.register("my_etl", script)

    from jobs.single_etl import run

    result = run(spark, "my_etl")

    script.run.assert_called_once()
    assert script.run.call_args[0][0] is spark
    assert script.run.call_args[1]["load_type"] is None
    assert "my_etl" in result
    assert "HISTORICAL: wrote to geo.example_target" in result


@patch("jobs.single_etl.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("jobs.single_etl.AppConfig.from_json", return_value=mock_app_config(logging=True))
def test_single_etl_passes_load_type(mock_app, mock_db):
    spark = MagicMock()
    script = MagicMock()
    script.run.return_value = EtlOutcome(True, "INCREMENTAL: wrote to geo.example_target")
    SingleEtlFactory.register("my_etl", script)

    from jobs.single_etl import run

    run(spark, "my_etl", load_type=LoadType.HISTORICAL)

    assert script.run.call_args[1]["load_type"] is LoadType.HISTORICAL


@patch("jobs.single_etl.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("jobs.single_etl.AppConfig.from_json", return_value=mock_app_config(logging=True))
def test_single_etl_passes_logger(mock_app, mock_db):
    spark = MagicMock()
    logger = MagicMock()
    script = MagicMock()
    SingleEtlFactory.register("my_etl", script)

    from jobs.single_etl import run

    run(spark, "my_etl", logger=logger)

    assert script.run.call_args[1]["logger"] is logger


@patch("jobs.single_etl.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("jobs.single_etl.AppConfig.from_json", return_value=mock_app_config(logging=True))
def test_single_etl_unknown_script_raises(mock_app, mock_db):
    from jobs.single_etl import run

    with pytest.raises(ValueError, match="No single ETL script for 'missing'"):
        run(MagicMock(), "missing")


@patch("jobs.single_etl.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("jobs.single_etl.AppConfig.from_json", return_value=mock_app_config(logging=True))
def test_single_etl_loads_configs(mock_app, mock_db):
    SingleEtlFactory.register("my_etl", MagicMock(return_value="done"))

    from jobs.single_etl import run

    run(MagicMock(), "my_etl")

    mock_app.assert_called_once_with()
    mock_db.assert_called_once_with()


@patch("jobs.single_etl.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("jobs.single_etl.AppConfig.from_json", return_value=mock_app_config(logging=True))
def test_single_etl_fails_when_nothing_loaded(mock_app, mock_db):
    """No record in the target table → RuntimeError, so the E2E chain stops."""
    # Arrange
    spark = MagicMock()
    logger = MagicMock()
    script = MagicMock()
    script.run.return_value = EtlOutcome(False, "No records loaded into example_etl — no rows to process in example")
    SingleEtlFactory.register("my_etl", script)

    from jobs.single_etl import run

    # Act & Assert
    with pytest.raises(RuntimeError, match="No records loaded into example_etl"):
        run(spark, "my_etl", logger=logger)

    logger.warning.assert_called_once_with("No records loaded into example_etl — no rows to process in example")
