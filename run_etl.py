import argparse

import findspark
findspark.init()

from pyspark.sql import SparkSession

from model.config import AppConfig
from model.enums import JobType
from factory import JobFactory


def main():
    # Load app config (Spark master, DB schema, JDBC driver)
    config = AppConfig.from_json()

    # Parse CLI arguments (--job CREATE --table_name <name>)
    parser = argparse.ArgumentParser(description="GEO ETL runner")
    parser.add_argument(
        "--job",
        type=lambda v: JobType(v.upper()),
        choices=list(JobType),
        required=True,
        help=f"Job type: {[j.value for j in JobType]}",
    )
    parser.add_argument("--table_name", required=True, help="Table name, e.g. avalanche_org_raw")
    args = parser.parse_args()

    # Resolve job module from registry (e.g. JobType.CREATE -> create_table module)
    job_module = JobFactory.create(args.job)

    # Connect to Spark cluster
    spark = SparkSession.builder \
        .master(config.spark.master) \
        .appName(f"geo_etl_{args.job.value.lower()}_{args.table_name}") \
        .getOrCreate()

    try:
        # Execute the job (e.g. create table in Supabase)
        result = job_module.run(spark, args.table_name)
        print(result)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
