import logging
import os
import sys

from utils.logger import setup_logger


def test_setup_logger_creates_log_file(tmp_path):
    log_file = str(tmp_path / "logs" / "test.log")
    logger = setup_logger(name="test_logger", log_file=log_file)
    assert os.path.exists(log_file)
    assert logger.name == "test_logger"
    assert logger.level == logging.INFO


def test_setup_logger_creates_nested_directory(tmp_path):
    log_file = str(tmp_path / "a" / "b" / "c" / "test.log")
    setup_logger(name="test_nested", log_file=log_file)
    assert os.path.exists(log_file)


def test_setup_logger_writes_log_entry(tmp_path):
    log_file = str(tmp_path / "test.log")
    logger = setup_logger(name="test_write", log_file=log_file)
    logger.info("hello world")
    with open(log_file) as f:
        content = f.read()
    assert "INFO" in content
    assert "hello world" in content


def test_setup_logger_avoids_duplicate_handlers(tmp_path):
    log_file = str(tmp_path / "test.log")
    logger1 = setup_logger(name="test_dedup", log_file=log_file)
    logger2 = setup_logger(name="test_dedup", log_file=log_file)
    file_handlers = [h for h in logger1.handlers if isinstance(h, logging.FileHandler)]
    assert len(file_handlers) == 1
    assert logger1 is logger2


def test_setup_logger_different_files(tmp_path):
    log_file1 = str(tmp_path / "a.log")
    log_file2 = str(tmp_path / "b.log")
    logger = setup_logger(name="test_multi", log_file=log_file1)
    setup_logger(name="test_multi", log_file=log_file2)
    file_handlers = [h for h in logger.handlers if isinstance(h, logging.FileHandler)]
    assert len(file_handlers) == 2


def test_setup_logger_mirrors_to_stdout(tmp_path):
    """A plain StreamHandler on sys.stdout is added so orchestrators (Airflow)
    capture the same content as the log file."""
    log_file = str(tmp_path / "test.log")
    logger = setup_logger(name="test_stdout", log_file=log_file)
    stream_handlers = [
        h for h in logger.handlers if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
    ]
    assert len(stream_handlers) == 1
    assert stream_handlers[0].stream is sys.stdout


def test_setup_logger_no_duplicate_stdout_handlers(tmp_path):
    """Repeated setup_logger calls with the same name add the stdout handler once."""
    log_file = str(tmp_path / "test.log")
    logger = setup_logger(name="test_stdout_dedup", log_file=log_file)
    setup_logger(name="test_stdout_dedup", log_file=log_file)
    stream_handlers = [
        h for h in logger.handlers if isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler)
    ]
    assert len(stream_handlers) == 1


def test_setup_logger_survives_concurrent_directory_creation(tmp_path, monkeypatch):
    """A concurrent task creating the shared job folder must not crash the logger.

    Airflow starts several ETL tasks at once and they all log to the same folder
    (e.g. ``logs/ingestion``).  Simulate the TOCTOU window: the folder exists but the
    logger's existence check still reports it missing, as if another task had just
    created it between check and ``makedirs``.
    """
    log_folder = tmp_path / "logs" / "ingestion"
    log_folder.mkdir(parents=True)
    real_exists = os.path.exists

    def racing_exists(path):
        if os.path.abspath(path) == os.path.abspath(log_folder):
            return False
        return real_exists(path)

    monkeypatch.setattr("utils.logger.os.path.exists", racing_exists)
    log_file = str(log_folder / "test.log")

    logger = setup_logger(name="test_race", log_file=log_file)
    logger.info("survived the race")

    assert os.path.exists(log_file)
    with open(log_file) as f:
        assert "survived the race" in f.read()
