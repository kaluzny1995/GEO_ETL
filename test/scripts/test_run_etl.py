from unittest.mock import MagicMock, patch
import pytest

from model.enums import JobType


def _mock_app_config():
    cfg = MagicMock()
    cfg.logging.directory = "logs"
    cfg.logging.level_int = 20
    cfg.spark.master = "spark://localhost:7078"
    cfg.spark.app_name_prefix = "GEO_ETL"
    return cfg


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_main_dispatches_create_job(mock_logger, mock_create, mock_config, mock_spark):
    mock_create.return_value = MagicMock(run=lambda s, t, logger=None: "done")

    import sys
    with patch.object(sys, "argv", ["run_etl.py", "CREATE", "avalanche_org_raw"]):
        from run_etl import main
        main()

    mock_create.assert_called_once_with(JobType.CREATE)


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_main_dispatches_ingestion_job(mock_logger, mock_create, mock_config, mock_spark):
    mock_create.return_value = MagicMock(run=lambda s, t, logger=None: "done")

    import sys
    with patch.object(sys, "argv", ["run_etl.py", "INGESTION", "avalanche_org_raw"]):
        from run_etl import main
        main()

    mock_create.assert_called_once_with(JobType.INGESTION)


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_main_passes_table_name_to_job(mock_logger, mock_create, mock_config, mock_spark):
    job_mock = MagicMock(run=MagicMock(return_value="done"))
    mock_create.return_value = job_mock

    import sys
    with patch.object(sys, "argv", ["run_etl.py", "CREATE", "my_custom_table"]):
        from run_etl import main
        main()

    job_mock.run.assert_called_once()
    assert job_mock.run.call_args[0][1] == "my_custom_table"


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_main_creates_spark_session(mock_logger, mock_create, mock_config, mock_spark):
    mock_create.return_value = MagicMock(run=lambda s, t, logger=None: "done")

    import sys
    with patch.object(sys, "argv", ["run_etl.py", "CREATE", "t"]):
        from run_etl import main
        main()

    mock_spark.builder.master.assert_called_once_with("spark://localhost:7078")
    mock_spark.builder.master.return_value.appName.return_value.getOrCreate.assert_called_once()


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_main_stops_spark_after_job(mock_logger, mock_create, mock_config, mock_spark):
    mock_create.return_value = MagicMock(run=lambda s, t, logger=None: "done")

    import sys
    with patch.object(sys, "argv", ["run_etl.py", "CREATE", "t"]):
        from run_etl import main
        main()

    spark_instance = mock_spark.builder.master.return_value.appName.return_value.getOrCreate.return_value
    spark_instance.stop.assert_called_once()


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_main_stops_spark_on_exception(mock_logger, mock_create, mock_config, mock_spark):
    mock_create.return_value = MagicMock(run=MagicMock(side_effect=RuntimeError("boom")))

    import sys
    with patch.object(sys, "argv", ["run_etl.py", "CREATE", "t"]):
        from run_etl import main
        with pytest.raises(RuntimeError, match="boom"):
            main()

    spark_instance = mock_spark.builder.master.return_value.appName.return_value.getOrCreate.return_value
    spark_instance.stop.assert_called_once()


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_main_passes_logger_to_job(mock_logger, mock_create, mock_config, mock_spark):
    job_mock = MagicMock(run=MagicMock(return_value="done"))
    mock_create.return_value = job_mock

    import sys
    with patch.object(sys, "argv", ["run_etl.py", "CREATE", "t"]):
        from run_etl import main
        main()

    job_mock.run.assert_called_once()
    assert job_mock.run.call_args.kwargs.get("logger") is mock_logger.return_value


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_main_invalid_job_type_exits(mock_logger, mock_create, mock_config, mock_spark):
    import sys
    with patch.object(sys, "argv", ["run_etl.py", "INVALID", "t"]):
        from run_etl import main
        with pytest.raises(SystemExit):
            main()
