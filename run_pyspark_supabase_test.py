import findspark
findspark.init()

import json
import os
from pyspark.sql import SparkSession

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(SCRIPT_DIR, "supabase_config.json")
SPARK_MASTER = "spark://localhost:7078"

with open(CONFIG_PATH) as f:
    config = json.load(f)

jdbc_url = f"jdbc:postgresql://{config['host']}:{config['port']}/{config['database']}"
properties = {
    "user": config["username"],
    "password": config["password"],
    "driver": "org.postgresql.Driver",
}

spark = SparkSession.builder \
    .master(SPARK_MASTER) \
    .appName("pyspark_supabase_test") \
    .getOrCreate()

print(f"Connected to:   {spark.sparkContext.master}")
print(f"Spark version:  {spark.version}")
print(f"Supabase host:  {config['host']}")
print(f"JDBC URL:       {jdbc_url}")
print()

try:
    tables_df = spark.read.jdbc(
        url=jdbc_url,
        table="information_schema.tables",
        properties=properties,
    )
    tables_df \
        .filter(~tables_df.table_schema.startswith("pg_")) \
        .groupBy("table_schema") \
        .count() \
        .orderBy("table_schema") \
        .withColumnRenamed("table_schema", "schema") \
        .withColumnRenamed("count", "tables") \
        .show(n=100, truncate=False)
finally:
    spark.stop()
    print("Done.")
