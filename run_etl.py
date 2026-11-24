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
import heapq
import os
import re
import tempfile
from contextlib import ExitStack

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


def _parse_ts(line: str) -> str:
    """Return the leading ``YYYY-MM-DD HH:MM:SS`` timestamp, or an empty string."""
    match = re.match(r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})", line)
    return match.group(1) if match else ""


def _iter_log_entries(lines, *, prefix: str = "", is_extra: bool = False):
    """Yield ``(timestamp, rendered_line, is_extra)`` for the non-empty *lines*.

    Parsing the timestamp separately from the line keeps the merge key cheap and
    lets a caller stream lines from a file without materializing the file.
    """
    for line in lines:
        stripped = line.rstrip()
        if stripped:
            yield _parse_ts(stripped), f"{prefix}{stripped}", is_extra


def _merge_logs_by_timestamp(
    main_log: str,
    extra_lines: list[str],
    memory_log: str,
    *,
    mirror_to_stdout: bool = False,
) -> list[str]:
    """Rewrite *main_log* with Python, Spark stderr and memory lines merged by timestamp.

    Both Python's ``logging`` and Java's ``StageMemoryListener`` use the
    ``YYYY-MM-DD HH:MM:SS`` format (Python adds milliseconds after a comma,
    Java does not). The main log and the memory log are chronological; the
    captured Spark stderr block is sorted once. All three are then merged as
    streams with :func:`heapq.merge` instead of being read into one list and
    sorted, keeping peak memory at one line per source rather than two copies
    of the whole task log (Spark stderr for a 30-minute job can be tens of
    megabytes). The *memory_log* file is deleted afterward.

    Args:
        main_log: Python task log to rewrite (read and replace atomically).
        extra_lines: Captured Spark stderr lines, in capture order.
        memory_log: Stage-memory log written by the Java listener, if present.
        mirror_to_stdout: Print the Spark/memory lines while merging instead of
            collecting them. ``main()`` uses this so the Airflow task log gets
            the same lines without a second in-memory list; the default keeps
            the historical return value for callers that need it.

    Returns:
        The extra lines (Spark stderr with a ``[spark]`` prefix plus the memory
        lines, in timestamp order) when *mirror_to_stdout* is false; otherwise
        an empty list because the lines were already written to stdout.
    """
    main_dir = os.path.dirname(os.path.abspath(main_log))
    merged_path: str | None = None
    extras: list[str] = []
    memory_exists = os.path.exists(memory_log)
    try:
        with ExitStack() as stack, open(main_log) as main_handle:
            memory_handle = stack.enter_context(open(memory_log)) if memory_exists else None
            memory_lines = _iter_log_entries(memory_handle, is_extra=True) if memory_handle else iter(())
            # Spark stderr is captured as one block and is not guaranteed to be in
            # timestamp order (Log4j lines without a timestamp sort first), so sort
            # this input once; the file streams remain lazy.
            spark_lines = sorted(extra_lines, key=_parse_ts)
            merged = heapq.merge(
                _iter_log_entries(main_handle),
                _iter_log_entries(spark_lines, prefix="[spark] ", is_extra=True),
                memory_lines,
                key=lambda entry: entry[0],
            )
            with tempfile.NamedTemporaryFile(
                mode="w", dir=main_dir, prefix=".merged-", suffix=".log", delete=False
            ) as output:
                merged_path = output.name
                for _, line, is_extra in merged:
                    output.write(line + "\n")
                    if is_extra:
                        if mirror_to_stdout:
                            print(line, flush=True)
                        else:
                            extras.append(line)
        os.replace(merged_path, main_log)
        merged_path = None
    finally:
        if memory_exists:
            os.remove(memory_log)
        if merged_path is not None and os.path.exists(merged_path):
            os.unlink(merged_path)
    return extras


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
            # Stream the JVM/fd-redirected lines to stdout for the orchestrator's task
            # log (Airflow) — the Python lines already stream there live. Mirroring
            # during the merge avoids building a second in-memory copy of Spark stderr.
            _merge_logs_by_timestamp(main_log, spark_lines, memory_log, mirror_to_stdout=True)


if __name__ == "__main__":
    main()
