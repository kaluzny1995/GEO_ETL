import argparse

import findspark
findspark.init()

from pyspark.sql import SparkSession

from model.config import AppConfig, SupabaseConnectionConfig
from utils.ddl_generator import drop_schema, _schema_has_tables


def main():
    # Load configs
    app_config = AppConfig.from_json()
    db_config = SupabaseConnectionConfig.from_json()

    # Parse CLI arguments
    parser = argparse.ArgumentParser(description="Drop a schema from Supabase (only if empty)")
    parser.add_argument("--schema_name", required=True, help="Schema name to drop")
    args = parser.parse_args()

    # Build JDBC connection params
    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(app_config.database.jdbc_driver)

    # Connect to Spark cluster
    spark = SparkSession.builder \
        .master(app_config.spark.master) \
        .appName(f"geo_etl_drop_schema_{args.schema_name}") \
        .getOrCreate()

    try:
        # Check if schema has tables
        if _schema_has_tables(spark, args.schema_name, jdbc_url, properties):
            print(f"Schema '{args.schema_name}' contains tables. Drop tables first.")
        else:
            # Drop the schema
            drop_schema(spark, args.schema_name, jdbc_url, properties)
            print(f"Schema '{args.schema_name}' dropped.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
