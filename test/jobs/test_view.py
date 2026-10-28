from unittest.mock import MagicMock, patch

import pytest

from factory import ViewScriptFactory
from test.conftest import mock_app_config, mock_db_config
from utils.etl_utils import EtlOutcome


@pytest.fixture(autouse=True)
def _clean_registry():
    ViewScriptFactory.clear()
    yield
    ViewScriptFactory.clear()


@patch("jobs.view.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("jobs.view.AppConfig.from_json", return_value=mock_app_config(logging=True))
def test_view_runs_registered_script(mock_app, mock_db):
    spark = MagicMock()
    script = MagicMock()
    script.run.return_value = EtlOutcome(True, "HISTORICAL: defined view geo.example_vw over geo.example_etl")
    ViewScriptFactory.register("my_vw", script)

    from jobs.view import run

    result = run(spark, "my_vw")

    script.run.assert_called_once()
    assert script.run.call_args[0][0] is spark
    assert "my_vw" in result
    assert "HISTORICAL: defined view geo.example_vw over geo.example_etl" in result


@patch("jobs.view.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("jobs.view.AppConfig.from_json", return_value=mock_app_config(logging=True))
def test_view_passes_logger(mock_app, mock_db):
    spark = MagicMock()
    logger = MagicMock()
    script = MagicMock()
    ViewScriptFactory.register("my_vw", script)

    from jobs.view import run

    run(spark, "my_vw", logger=logger)

    assert script.run.call_args[1]["logger"] is logger


@patch("jobs.view.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("jobs.view.AppConfig.from_json", return_value=mock_app_config(logging=True))
def test_view_passes_no_load_type(mock_app, mock_db):
    """Views are HISTORICAL-only — the job never hands a load type to the script."""
    script = MagicMock()
    ViewScriptFactory.register("my_vw", script)

    from jobs.view import run

    run(MagicMock(), "my_vw")

    assert "load_type" not in script.run.call_args[1]


@patch("jobs.view.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("jobs.view.AppConfig.from_json", return_value=mock_app_config(logging=True))
def test_view_unknown_script_raises(mock_app, mock_db):
    from jobs.view import run

    with pytest.raises(ValueError, match="No view script for 'missing'"):
        run(MagicMock(), "missing")


@patch("jobs.view.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("jobs.view.AppConfig.from_json", return_value=mock_app_config(logging=True))
def test_view_loads_configs(mock_app, mock_db):
    ViewScriptFactory.register("my_vw", MagicMock())

    from jobs.view import run

    run(MagicMock(), "my_vw")

    mock_app.assert_called_once_with()
    mock_db.assert_called_once_with()


@patch("jobs.view.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch("jobs.view.AppConfig.from_json", return_value=mock_app_config(logging=True))
def test_view_fails_when_query_returns_no_rows(mock_app, mock_db):
    """No rows from the query → RuntimeError, so the E2E chain stops and the view stays untouched."""
    # Arrange
    spark = MagicMock()
    logger = MagicMock()
    script = MagicMock()
    script.run.return_value = EtlOutcome(
        False, "No records loaded into example_vw — query returned no rows from example_etl"
    )
    ViewScriptFactory.register("my_vw", script)

    from jobs.view import run

    # Act & Assert
    with pytest.raises(RuntimeError, match="No records loaded into example_vw"):
        run(spark, "my_vw", logger=logger)

    logger.warning.assert_called_once_with(
        "No records loaded into example_vw — query returned no rows from example_etl"
    )
