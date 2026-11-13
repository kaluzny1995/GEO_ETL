"""Main CLI entry point for GEO_ETL jobs.

Usage::

    python run_etl.py CREATE <table_name>
    python run_etl.py INGESTION <table_name>
    python run_etl.py DATA_QUALITY <table_name>
    python run_etl.py DATA_QUALITY <table_name> --overall
    python run_etl.py DATA_SOURCE_FILE <source_name> [--load-type auto|historical|incremental]
    python run_etl.py SINGLE_ETL <script_name> [--load-type auto|historical|incremental]
    python run_etl.py VIEW <script_name>
    python run_etl.py PREDICTION prediction_etl
"""

import argparse
import datetime as dt
import os
import tempfile

import findspark

findspark.init()

from pyspark.sql import SparkSession

import jobs  # noqa: F401 — registers jobs via jobs/__init__.py
import model  # noqa: F401 — registers tables via model/__init__.py
from factory import JobFactory
from model.config import AppConfig
from model.enums import JobType, LoadType
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
        # Kept open on purpose: stop_and_read() closes it after restoring fd 2.
        self._tmp = tempfile.NamedTemporaryFile(  # noqa: SIM115
            mode="w",
            suffix=".spark_stderr",
            delete=False,
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


def _merge_logs_by_timestamp(main_log: str, extra_lines: list[str], memory_log: str) -> list[str]:
    """Rewrite *main_log* with lines from Python, Spark stderr, and memory merged by timestamp.

    Both Python's ``logging`` and Java's ``StageMemoryListener`` use the
    ``YYYY-MM-DD HH:MM:SS`` format (Python adds milliseconds after a comma,
    Java does not).  This function reads the existing *main_log*, appends
    *extra_lines* (Spark stderr captured via fd redirect), reads the
    *memory_log*, parses timestamps, and rewrites *main_log* in chronological
    order.  The *memory_log* file is deleted afterward.

    Returns the extra lines (Spark stderr with a ``[spark]`` prefix plus the
    memory lines, in timestamp order) so the caller can mirror them to stdout —
    the Python lines already stream there live via the logger's stdout handler,
    but the JVM/fd-redirected lines never pass through it.
    """
    import re

    def _parse_ts(line: str) -> str:
        m = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
        return m.group(1) if m else ""

    # Collect all (timestamp_key, full_line) pairs
    entries: list[tuple[str, str]] = []
    extras: list[tuple[str, str]] = []

    # Lines already in main_log
    with open(main_log) as f:
        for line in f:
            entries.append((_parse_ts(line), line))

    # Spark stderr lines — prefix and add
    for line in extra_lines:
        stripped = line.rstrip()
        if stripped:
            full_line = f"[spark] {stripped}"
            entries.append((_parse_ts(stripped), full_line))
            extras.append((_parse_ts(stripped), full_line))

    # Memory log lines
    if os.path.exists(memory_log):
        with open(memory_log) as f:
            for line in f:
                stripped = line.rstrip()
                if stripped:
                    entries.append((_parse_ts(stripped), stripped))
                    extras.append((_parse_ts(stripped), stripped))
        os.remove(memory_log)

    # Stable sort by timestamp (preserves original order for equal timestamps)
    entries.sort(key=lambda e: e[0])
    extras.sort(key=lambda e: e[0])

    with open(main_log, "w") as f:
        for _, line in entries:
            f.write(line + "\n")

    return [line for _, line in extras]


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
    parser.add_argument(
        "table_name",
        help=(
            "Table name, e.g. avalanche_org (INGESTION writes directly to the plain table); "
            "SINGLE_ETL expects the name of a script from single_etl/, "
            "VIEW expects the name of a script from views/ (HISTORICAL load only); "
            "PREDICTION expects prediction_etl"
        ),
    )
    parser.add_argument(
        "--overall",
        action="store_true",
        help="DATA_QUALITY only: measure the whole table as one snapshot stored under file_date = 'overall'",
    )
    parser.add_argument(
        "--load-type",
        choices=["auto", "historical", "incremental"],
        default="auto",
        help=(
            "SINGLE_ETL / DATA_SOURCE_FILE only: force the load type "
            "(default: auto — the job detects it from its target)"
        ),
    )
    args = parser.parse_args()

    if args.overall and args.job is not JobType.DATA_QUALITY:
        parser.error("--overall can only be used with the DATA_QUALITY job")

    if args.load_type != "auto" and args.job not in (JobType.SINGLE_ETL, JobType.DATA_SOURCE_FILE):
        parser.error("--load-type can only be used with the SINGLE_ETL or DATA_SOURCE_FILE job")

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

    # Connect to Spark cluster. The memory and partition caps come from config.json
    # (overridable through SPARK_* environment variables in Docker) so a single ETL job
    # cannot reserve the whole host.
    builder = SparkSession.builder.master(config.spark.master).appName(f"{app_prefix}_{job_name}_{table_name}")
    for key, value in config.spark.session_options.items():
        builder = builder.config(key, value)
    # Spark ML writes model files through the Hadoop local FS, from executor
    # processes that may use a different UID than the driver (Compose workers vs
    # the Airflow scheduler). World-writable model directories keep the shared
    # ml_models bind mount usable by both.
    if args.job is JobType.PREDICTION:
        builder = builder.config("spark.hadoop.fs.permissions.umask-mode", "000")
    spark = builder.getOrCreate()

    # Register stage-level memory logger
    memory_log = os.path.join(log_dir, "memory", f"stage_memory_{dt.datetime.now():%Y%m%d_%H%M%S}.log")
    register_memory_logger(spark, memory_log)

    try:
        logger.info(f"Connected to Spark: {spark.sparkContext.master}")
        logger.info(f"Spark version: {spark.version}")

        # Execute the job (e.g. create table in Supabase)
        job_kwargs: dict[str, object] = {}
        if args.overall:
            job_kwargs["overall"] = True
        if args.job in (JobType.SINGLE_ETL, JobType.DATA_SOURCE_FILE):
            job_kwargs["load_type"] = None if args.load_type == "auto" else LoadType(args.load_type.upper())
        result = job_module.run(spark, table_name, logger=logger, **job_kwargs)
        logger.info(result)
    except Exception:
        logger.exception("ETL job failed")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")

        # Restore stderr and collect captured Spark logs
        spark_lines = stderr_capture.stop_and_read()

        # Find the main log file (the last handler's filename)
        main_log = None
        for h in logger.handlers:
            if hasattr(h, "baseFilename"):
                main_log = h.baseFilename

        if main_log and (spark_lines or os.path.exists(memory_log)):
            # Mirror the JVM/fd-redirected lines to stdout for the orchestrator's
            # task log (Airflow) — the Python lines already stream there live.
            for line in _merge_logs_by_timestamp(main_log, spark_lines, memory_log):
                print(line, flush=True)


if __name__ == "__main__":
    main()
