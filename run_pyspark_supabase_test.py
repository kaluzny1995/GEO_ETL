import findspark
findspark.init()

from pyspark.sql import SparkSession

from model.config import AppConfig, SupabaseConnectionConfig

app_config = AppConfig.from_json()
db_config = SupabaseConnectionConfig.from_json()

jdbc_url = db_config.get_jdbc_url()
properties = db_config.get_jdbc_properties(app_config.database.jdbc_driver)

spark = SparkSession.builder \
    .master(app_config.spark.master) \
    .appName("pyspark_supabase_test") \
    .getOrCreate()

print(f"Connected to:   {spark.sparkContext.master}")
print(f"Spark version:  {spark.version}")
print(f"Supabase host:  {db_config.host}")
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
