import findspark
findspark.init()

import os
import datetime as dt
from pyspark.sql import SparkSession  # type: ignore[import-untyped]
from pyspark import SparkContext  # type: ignore[import-untyped]
from py4j.java_gateway import JavaGateway, JVMView  # type: ignore[import-untyped]

from model.config import AppConfig
from utils.logger import setup_logger


def main():
    config = AppConfig.from_json()

    log_dir = config.logging.directory

    logger = setup_logger(
        name="pyspark_csv_test",
        log_file=os.path.join(log_dir, "pyspark_csv_test", f"pyspark_csv_test_{dt.datetime.now().isoformat()}.log"),
        level=config.logging.level_int,
    )

    SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
    DATA_PATH = os.path.normpath(os.path.join(SCRIPT_DIR, config.data.directory))

    spark = SparkSession.builder \
        .master(config.spark.master) \
        .appName("pyspark_csv_test") \
        .getOrCreate()

    sc: SparkContext = spark.sparkContext
    jvm: JVMView = sc._jvm  # type: ignore[assignment]
    jsc = sc._jsc  # type: ignore[attr-defined]

    from py4j.java_gateway import java_import
    java_import(jvm, "org.apache.hadoop.fs.*")

    fs = jvm.FileSystem.get(jsc.hadoopConfiguration())
    Path = jvm.Path

    logger.info(f"Connected to: {sc.master}")
    logger.info(f"Spark version: {spark.version}")
    logger.info(f"Data path:     {DATA_PATH}")

    try:
        statuses = fs.listStatus(Path(DATA_PATH))
        if not statuses:
            logger.warning(f"No entries found in {DATA_PATH}")
        else:
            for status in sorted(statuses, key=lambda s: s.getPath().getName()):
                name = status.getPath().getName()
                if status.isDirectory():
                    inner = fs.listStatus(Path(f"{DATA_PATH}/{name}"))
                    csv_count = sum(1 for s in inner if s.getPath().getName().endswith(".csv"))
                    if csv_count > 0:
                        logger.info(f"  {name:30s} -> {csv_count} CSV file(s)")
                    else:
                        logger.info(f"  {name:30s} -> {len(inner)} non-CSV file(s)")
                else:
                    logger.info(f"  {name:30s} -> (file)")
    finally:
        spark.stop()
        logger.info("Done.")


if __name__ == "__main__":
    main()
