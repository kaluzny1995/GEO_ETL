import logging
import os
import sys


def setup_logger(name: str, log_file: str, level: int = logging.INFO) -> logging.Logger:
    """Setup logger — writes to *log_file* and mirrors every record to stdout so
    subprocess orchestrators (e.g. Airflow task logs) capture the full job output.
    Avoids duplicate handlers when called multiple times with the same name
    (e.g. periodic Celery task invocations).

    The log directory is created with ``exist_ok=True``: Airflow runs several ETL
    tasks concurrently and they all share the per-job folder (e.g. ``logs/ingestion``),
    so a check-then-create sequence would race and one task would crash with
    ``FileExistsError`` when another created the folder first."""
    log_file_folder: str = os.path.dirname(log_file)
    if log_file_folder:
        os.makedirs(log_file_folder, exist_ok=True)

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
