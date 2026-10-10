from unittest.mock import MagicMock, patch

import pytest

from model.enums import JobType, LoadType


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

    with patch.object(sys, "argv", ["run_etl.py", "INGESTION", "avalanche_org"]):
        from run_etl import main

        main()

    mock_create.assert_called_once_with(JobType.INGESTION)


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_main_dispatches_data_quality_job(mock_logger, mock_create, mock_config, mock_spark):
    mock_create.return_value = MagicMock(run=lambda s, t, logger=None: "done")

    import sys

    with patch.object(sys, "argv", ["run_etl.py", "DATA_QUALITY", "avalanche_org"]):
        from run_etl import main

        main()

    mock_create.assert_called_once_with(JobType.DATA_QUALITY)


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_data_quality_passes_plain_table_name(mock_logger, mock_create, mock_config, mock_spark):
    job_mock = MagicMock(run=MagicMock(return_value="done"))
    mock_create.return_value = job_mock

    import sys

    with patch.object(sys, "argv", ["run_etl.py", "DATA_QUALITY", "avalanche_org"]):
        from run_etl import main

        main()

    assert job_mock.run.call_args[0][1] == "avalanche_org"


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


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_ingestion_log_uses_name_without_raw(mock_logger, mock_create, mock_config, mock_spark):
    mock_create.return_value = MagicMock(run=lambda s, t, logger=None: "done")

    import sys

    with patch.object(sys, "argv", ["run_etl.py", "INGESTION", "avalanche_org"]):
        from run_etl import main

        main()

    log_file_arg = mock_logger.call_args.kwargs.get("log_file") or mock_logger.call_args[0][1]
    assert "avalanche_org" in log_file_arg
    assert "avalanche_org_raw" not in log_file_arg


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_ingestion_passes_raw_name_to_job(mock_logger, mock_create, mock_config, mock_spark):
    job_mock = MagicMock(run=MagicMock(return_value="done"))
    mock_create.return_value = job_mock

    import sys

    with patch.object(sys, "argv", ["run_etl.py", "INGESTION", "avalanche_org"]):
        from run_etl import main

        main()

    assert job_mock.run.call_args[0][1] == "avalanche_org_raw"


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_create_log_keeps_raw_suffix(mock_logger, mock_create, mock_config, mock_spark):
    mock_create.return_value = MagicMock(run=lambda s, t, logger=None: "done")

    import sys

    with patch.object(sys, "argv", ["run_etl.py", "CREATE", "avalanche_org_raw"]):
        from run_etl import main

        main()

    log_file_arg = mock_logger.call_args.kwargs.get("log_file") or mock_logger.call_args[0][1]
    assert "avalanche_org_raw" in log_file_arg


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_data_quality_passes_overall_flag(mock_logger, mock_create, mock_config, mock_spark):
    job_mock = MagicMock(run=MagicMock(return_value="done"))
    mock_create.return_value = job_mock

    import sys

    with patch.object(sys, "argv", ["run_etl.py", "DATA_QUALITY", "avalanche_org", "--overall"]):
        from run_etl import main

        main()

    assert job_mock.run.call_args.kwargs["overall"] is True


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_overall_defaults_to_false(mock_logger, mock_create, mock_config, mock_spark):
    job_mock = MagicMock(run=MagicMock(return_value="done"))
    mock_create.return_value = job_mock

    import sys

    with patch.object(sys, "argv", ["run_etl.py", "DATA_QUALITY", "avalanche_org"]):
        from run_etl import main

        main()

    assert "overall" not in job_mock.run.call_args.kwargs


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_overall_rejected_for_other_jobs(mock_logger, mock_create, mock_config, mock_spark):
    import sys

    with patch.object(sys, "argv", ["run_etl.py", "CREATE", "t", "--overall"]):
        from run_etl import main

        with pytest.raises(SystemExit) as exc:
            main()

    assert exc.value.code == 2
    mock_create.assert_not_called()


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_main_dispatches_single_etl_job(mock_logger, mock_create, mock_config, mock_spark):
    mock_create.return_value = MagicMock(run=MagicMock(return_value="done"))

    import sys

    with patch.object(sys, "argv", ["run_etl.py", "SINGLE_ETL", "avalanche_org_etl"]):
        from run_etl import main

        main()

    mock_create.assert_called_once_with(JobType.SINGLE_ETL)


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_single_etl_passes_script_name(mock_logger, mock_create, mock_config, mock_spark):
    job_mock = MagicMock(run=MagicMock(return_value="done"))
    mock_create.return_value = job_mock

    import sys

    with patch.object(sys, "argv", ["run_etl.py", "SINGLE_ETL", "my_dataset_etl"]):
        from run_etl import main

        main()

    assert job_mock.run.call_args[0][1] == "my_dataset_etl"


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_single_etl_defaults_to_auto_load_type(mock_logger, mock_create, mock_config, mock_spark):
    job_mock = MagicMock(run=MagicMock(return_value="done"))
    mock_create.return_value = job_mock

    import sys

    with patch.object(sys, "argv", ["run_etl.py", "SINGLE_ETL", "avalanche_org_etl"]):
        from run_etl import main

        main()

    assert job_mock.run.call_args.kwargs["load_type"] is None


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_single_etl_passes_forced_load_type(mock_logger, mock_create, mock_config, mock_spark):
    job_mock = MagicMock(run=MagicMock(return_value="done"))
    mock_create.return_value = job_mock

    import sys

    with patch.object(sys, "argv", ["run_etl.py", "SINGLE_ETL", "avalanche_org_etl", "--load-type", "historical"]):
        from run_etl import main

        main()

    assert job_mock.run.call_args.kwargs["load_type"] is LoadType.HISTORICAL


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_single_etl_incremental_load_type(mock_logger, mock_create, mock_config, mock_spark):
    job_mock = MagicMock(run=MagicMock(return_value="done"))
    mock_create.return_value = job_mock

    import sys

    with patch.object(sys, "argv", ["run_etl.py", "SINGLE_ETL", "avalanche_org_etl", "--load-type", "incremental"]):
        from run_etl import main

        main()

    assert job_mock.run.call_args.kwargs["load_type"] is LoadType.INCREMENTAL


@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_load_type_rejected_for_other_jobs(mock_logger, mock_create, mock_config, mock_spark):
    import sys

    with patch.object(sys, "argv", ["run_etl.py", "INGESTION", "t", "--load-type", "historical"]):
        from run_etl import main

        with pytest.raises(SystemExit) as exc:
            main()

    assert exc.value.code == 2
    mock_create.assert_not_called()


def test_stderr_capture_stop_and_read_without_start_returns_empty():
    from run_etl import _StderrCapture

    assert _StderrCapture().stop_and_read() == []


def test_merge_logs_by_timestamp_merges_python_spark_and_memory(tmp_path):
    from run_etl import _merge_logs_by_timestamp

    # Arrange
    main_log = tmp_path / "main.log"
    memory_log = tmp_path / "memory.log"
    main_log.write_text("2026-01-01 10:00:02 python line\n2026-01-01 10:00:04 python second\n")
    memory_log.write_text("2026-01-01 10:00:03 memory line\n")
    extra_lines = ["2026-01-01 10:00:01 spark line\n", "plain stderr tail\n"]

    # Act
    extras = _merge_logs_by_timestamp(str(main_log), extra_lines, str(memory_log))

    # Assert — content lines in timestamp order (main_log lines are re-written with a trailing blank)
    lines = [line for line in main_log.read_text().splitlines() if line]
    assert lines == [
        "[spark] plain stderr tail",
        "[spark] 2026-01-01 10:00:01 spark line",
        "2026-01-01 10:00:02 python line",
        "2026-01-01 10:00:03 memory line",
        "2026-01-01 10:00:04 python second",
    ]
    assert not memory_log.exists()
    # The extras (spark + memory, timestamp-ordered) are returned for the stdout mirror
    assert extras == [
        "[spark] plain stderr tail",
        "[spark] 2026-01-01 10:00:01 spark line",
        "2026-01-01 10:00:03 memory line",
    ]


@patch("run_etl.register_memory_logger")
@patch("run_etl._StderrCapture")
@patch("run_etl.SparkSession")
@patch("run_etl.AppConfig.from_json", return_value=_mock_app_config())
@patch("run_etl.JobFactory.create")
@patch("run_etl.setup_logger")
def test_main_merges_captured_spark_logs_into_main_log(
    mock_logger, mock_create, mock_config, mock_spark, mock_capture, mock_memory, capsys
):
    mock_create.return_value = MagicMock(run=lambda s, t, logger=None: "done")
    mock_capture.return_value.stop_and_read.return_value = ["2026-01-01 10:00:01 captured spark line"]

    # Arrange — a real log file the mocked logger reports as its active handler target
    import os
    import tempfile
    from types import SimpleNamespace

    with tempfile.NamedTemporaryFile(mode="w", suffix=".log", delete=False) as f:
        f.write("2026-01-01 10:00:00 job started\n")
        main_log_path = f.name
    logger_mock = MagicMock()
    logger_mock.handlers = [SimpleNamespace(baseFilename=main_log_path)]
    mock_logger.return_value = logger_mock

    import sys

    # Act
    with patch.object(sys, "argv", ["run_etl.py", "CREATE", "avalanche_org_raw"]):
        from run_etl import main

        main()

    # Assert — merged into the file …
    with open(main_log_path) as f:
        content = f.read()
    os.unlink(main_log_path)
    assert "[spark] 2026-01-01 10:00:01 captured spark line" in content
    assert "2026-01-01 10:00:00 job started" in content
    # … and mirrored to stdout for the orchestrator's task log (Airflow)
    assert "[spark] 2026-01-01 10:00:01 captured spark line" in capsys.readouterr().out
