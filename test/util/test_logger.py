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
