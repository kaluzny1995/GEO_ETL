"""Tests for the DATA_SOURCE_FILE job."""

from unittest.mock import MagicMock, patch

from model.enums import LoadType
from test.conftest import mock_app_config, mock_db_config
from utils.data_source_files import PublishResult


@patch("jobs.data_source_file.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch(
    "jobs.data_source_file.AppConfig.from_json",
    return_value=mock_app_config(logging=True, data_directory="../GEO_Download/data"),
)
@patch("jobs.data_source_file.publish_source_files")
def test_run_publishes_the_source_and_returns_a_summary(mock_publish, mock_app, mock_db) -> None:
    """The job delegates to the publisher and reports the load type and counts."""
    spark = MagicMock()
    log = MagicMock()
    mock_publish.return_value = PublishResult(
        source_name="EONET",
        load_type=LoadType.HISTORICAL,
        existing_files=0,
        written_files=3,
        removed_files=0,
    )

    from jobs.data_source_file import run

    result = run(spark, "EONET", logger=log)

    mock_publish.assert_called_once_with(
        spark,
        mock_app.return_value,
        mock_db.return_value,
        "EONET",
        logger=log,
        load_type=None,
    )
    assert "3 written" in result
    assert "0 removed" in result
    assert "HISTORICAL" in result


@patch("jobs.data_source_file.SupabaseConnectionConfig.from_env", return_value=mock_db_config())
@patch(
    "jobs.data_source_file.AppConfig.from_json",
    return_value=mock_app_config(logging=True, data_directory="../GEO_Download/data"),
)
@patch("jobs.data_source_file.publish_source_files")
def test_run_passes_a_forced_load_type(mock_publish, mock_app, mock_db) -> None:
    """The CLI's --load-type reaches the publisher."""
    mock_publish.return_value = PublishResult(
        source_name="EONET",
        load_type=LoadType.INCREMENTAL,
        existing_files=2,
        written_files=1,
        removed_files=0,
    )

    from jobs.data_source_file import run

    run(MagicMock(), "EONET", load_type=LoadType.INCREMENTAL)

    assert mock_publish.call_args.kwargs["load_type"] is LoadType.INCREMENTAL
