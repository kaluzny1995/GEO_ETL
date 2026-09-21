import argparse

import findspark
findspark.init()

from pyspark.sql import SparkSession

from model.config import AppConfig, SupabaseConnectionConfig
from utils.ddl_generator import drop_table


def main():
    # Load configs
    app_config = AppConfig.from_json()
    db_config = SupabaseConnectionConfig.from_json()

    # Parse CLI arguments
    parser = argparse.ArgumentParser(description="Drop a table from Supabase")
    parser.add_argument("--table_name", required=True, help="Table name to drop")
    args = parser.parse_args()

    # Build JDBC connection params
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(app_config.database.jdbc_driver)
    schema = app_config.database.db_schema

    # Connect to Spark cluster
    spark = SparkSession.builder \
        .master(app_config.spark.master) \
        .appName(f"geo_etl_drop_table_{args.table_name}") \
        .getOrCreate()

    try:
        # Drop the table
        drop_table(spark, args.table_name, schema, jdbc_url, properties)
        print(f"Table {schema}.{args.table_name} dropped.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
