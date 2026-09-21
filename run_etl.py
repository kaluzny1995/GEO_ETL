import argparse

import findspark
findspark.init()

from pyspark.sql import SparkSession

from model.config import AppConfig
from model.enums import JobType
from jobs.registry import JobFactory


def main():
    config = AppConfig.from_json()

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

    job_module = JobFactory.create(args.job)

    spark = SparkSession.builder \
        .master(config.spark.master) \
        .appName(f"geo_etl_{args.job.value.lower()}_{args.table_name}") \
        .getOrCreate()

    try:
        result = job_module.run(spark, args.table_name)
        print(result)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
