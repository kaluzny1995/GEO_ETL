import logging
import os
import sys


def setup_logger(name: str, log_file: str, level: int = logging.INFO) -> logging.Logger:
    """Setup logger — writes to *log_file* and mirrors every record to stdout so
    subprocess orchestrators (e.g. Airflow task logs) capture the full job output.
    Avoids duplicate handlers when called multiple times with the same name
    (e.g. periodic Celery task invocations)."""
    log_file_folder: str = os.path.dirname(log_file)
    if log_file_folder and not os.path.exists(log_file_folder):
        os.makedirs(log_file_folder)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    existing_paths = {os.path.abspath(h.baseFilename) for h in logger.handlers if isinstance(h, logging.FileHandler)}
    if os.path.abspath(log_file) not in existing_paths:
        handler = logging.FileHandler(log_file)
        handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(handler)

    # FileHandler is a StreamHandler subclass — only consider plain stream handlers
    has_stdout_handler = any(
        isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler) and h.stream is sys.stdout
        for h in logger.handlers
    )
    if not has_stdout_handler:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
        logger.addHandler(stream_handler)

    return logger
