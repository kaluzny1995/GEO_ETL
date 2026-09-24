"""Main CLI entry point for GEO_ETL jobs.

Usage::

    python run_etl.py CREATE <table_name>
    python run_etl.py INGESTION <table_name>
"""

import argparse
import datetime as dt
import os
import tempfile

import findspark
findspark.init()

from pyspark.sql import SparkSession

import model  # noqa: F401 — registers tables via model/__init__.py
import jobs  # noqa: F401 — registers jobs via jobs/__init__.py
from model.config import AppConfig
from model.enums import JobType
from factory import JobFactory
from utils.logger import setup_logger
from utils.spark_monitor import register_memory_logger


class _StderrCapture:
    """Redirect fd 2 (stderr) to a temp file so Spark Log4j output is captured.

    Spark's Log4j writes directly to the stderr file descriptor *before*
    PySpark's Python process can intercept ``sys.stderr``.  By dup2-ing
    fd 2 to a temporary file before the ``SparkSession`` is created, we
    capture all Spark-internal log lines.  After the session stops, the
    original fd is restored and the captured lines are written to the
    Python logger with a ``[spark]`` prefix.
    """

    def __init__(self):
        self._orig_fd = None
        self._tmp = None

    def start(self):
        self._orig_fd = os.dup(2)
        self._tmp = tempfile.NamedTemporaryFile(
            mode="w", suffix=".spark_stderr", delete=False,
        )
        fd = os.open(self._tmp.name, os.O_WRONLY | os.O_APPEND)
        os.dup2(fd, 2)
        os.close(fd)

    def stop_and_read(self) -> list[str]:
        if self._orig_fd is not None:
            os.dup2(self._orig_fd, 2)
            os.close(self._orig_fd)
            self._orig_fd = None
        if self._tmp is not None:
            path = self._tmp.name
            self._tmp.close()
            try:
                with open(path) as f:
                    lines = f.readlines()
            finally:
                os.unlink(path)
            return lines
        return []


def main() -> None:
    """Parse CLI arguments, connect to Spark, and execute the requested ETL job."""
    # Load app config (Spark master, DB schema, JDBC driver)
    config = AppConfig.from_json()

    # Parse CLI arguments (positional: job table_name)
    parser = argparse.ArgumentParser(description="GEO ETL runner")
    parser.add_argument(
        "job",
        type=lambda v: JobType(v.upper()),
        choices=list(JobType),
        help=f"Job type: {[j.value for j in JobType]}",
    )
    parser.add_argument("table_name", help="Table name, e.g. avalanche_org_raw")
    args = parser.parse_args()

    job_name = args.job.value.lower()
    table_name = args.table_name
    log_dir = config.logging.directory
    app_prefix = config.spark.app_name_prefix

    logger = setup_logger(
        name=f"etl_{job_name}_{table_name}",
        log_file=os.path.join(log_dir, job_name, f"{job_name}_{table_name}_{dt.datetime.now().isoformat()}.log"),
        level=config.logging.level_int,
    )
    logger.info(f"---- ETL job started: {job_name} {table_name} ----")

    # Resolve job module from registry (e.g. JobType.CREATE -> create_table module)
    job_module = JobFactory.create(args.job)

    # Capture Spark's stderr (Log4j) before creating the session
    stderr_capture = _StderrCapture()
    stderr_capture.start()

    # Connect to Spark cluster
    spark = SparkSession.builder \
        .master(config.spark.master) \
        .appName(f"{app_prefix}_{job_name}_{table_name}") \
        .getOrCreate()

    # Register stage-level memory logger
    memory_log = os.path.join(log_dir, "memory", f"stage_memory_{dt.datetime.now():%Y%m%d_%H%M%S}.log")
    register_memory_logger(spark, memory_log)
    logger.info(f"Stage memory logging to: {memory_log}")

    try:
        logger.info(f"Connected to Spark: {spark.sparkContext.master}")
        logger.info(f"Spark version: {spark.version}")

        # Execute the job (e.g. create table in Supabase)
        result = job_module.run(spark, table_name, logger=logger)
        logger.info(result)
    except Exception:
        logger.exception("ETL job failed")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")

        # Restore stderr and write captured Spark logs to the Python log
        spark_lines = stderr_capture.stop_and_read()
        if spark_lines:
            logger.info("---- Spark internal logs ----")
            for line in spark_lines:
                stripped = line.rstrip()
                if stripped:
                    logger.info(f"[spark] {stripped}")


if __name__ == "__main__":
    main()
