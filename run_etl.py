"""Main CLI entry point for GEO_ETL jobs.

Usage::

    python run_etl.py CREATE <table_name>
    python run_etl.py INGESTION <table_name>
"""

import argparse
import datetime as dt
import os

import findspark
findspark.init()

from pyspark.sql import SparkSession

import model  # noqa: F401 — registers tables via model/__init__.py
import jobs  # noqa: F401 — registers jobs via jobs/__init__.py
from model.config import AppConfig
from model.enums import JobType
from factory import JobFactory
from utils.logger import setup_logger


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

    # Connect to Spark cluster
    spark = SparkSession.builder \
        .master(config.spark.master) \
        .appName(f"{app_prefix}_{job_name}_{table_name}") \
        .getOrCreate()

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


if __name__ == "__main__":
    main()
