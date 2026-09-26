"""Spark memory monitoring via a Java SparkListener.

Registers a JVM-side listener that logs per-stage memory consumption
(JVM heap, Spark execution/storage memory, spill metrics, GC time)
to a log file whenever a stage completes.

Usage::

    from utils.spark_monitor import register_memory_logger

    register_memory_logger(spark, "logs/memory/stage_memory.log")
    # ... run Spark jobs ...
    # Stage memory lines appear automatically in the log file

Prerequisites:
    The compiled ``spark-monitor.jar`` must be on the driver classpath.
    It is automatically available when placed in PySpark's ``jars/``
    directory (done during project setup).
"""

import os

from pyspark.sql import SparkSession


def register_memory_logger(spark: SparkSession, log_file: str) -> None:
    """Register a Java SparkListener that logs memory on stage completion.

    Args:
        spark: Active Spark session.
        log_file: Absolute or relative path to the output log file.
            Directories are created automatically.
    """
    log_dir = os.path.dirname(log_file)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    jvm = spark.sparkContext._jvm
    if jvm is None:
        raise RuntimeError("Spark JVM gateway is not available — is the Spark session active?")
    listener = jvm.sparkmonitor.StageMemoryListener(os.path.abspath(log_file))
    jvm.sparkmonitor.ListenerHelper.addListener(spark.sparkContext._jsc.sc(), listener)
