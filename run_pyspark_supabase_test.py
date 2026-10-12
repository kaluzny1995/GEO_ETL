import findspark

findspark.init()

import datetime as dt
import os

from pyspark.sql import SparkSession

from model.config import AppConfig, SupabaseConnectionConfig
from utils.logger import setup_logger


def main() -> None:
    app_config = AppConfig.from_json()
    db_config = SupabaseConnectionConfig.from_env()

    log_dir = app_config.logging.directory

    logger = setup_logger(
        name="pyspark_supabase_test",
        log_file=os.path.join(
            log_dir, "pyspark_supabase_test", f"pyspark_supabase_test_{dt.datetime.now().isoformat()}.log"
        ),
        level=app_config.logging.level_int,
    )

    jdbc_url = db_config.get_jdbc_url()
    properties = db_config.get_jdbc_properties(app_config.database.jdbc_driver)

    spark = SparkSession.builder.master(app_config.spark.master).appName("pyspark_supabase_test").getOrCreate()

    logger.info(f"Connected to:   {spark.sparkContext.master}")
    logger.info(f"Spark version:  {spark.version}")
    logger.info(f"Supabase host:  {db_config.host}")
    logger.info(f"JDBC URL:       {jdbc_url}")

    try:
        tables_df = spark.read.jdbc(
            url=jdbc_url,
            table="information_schema.tables",
            properties=properties,
        )
        tables_df.filter(~tables_df.table_schema.startswith("pg_")).groupBy("table_schema").count().orderBy(
            "table_schema"
        ).withColumnRenamed("table_schema", "schema").withColumnRenamed("count", "tables").show(n=100, truncate=False)
    finally:
        spark.stop()
        logger.info("Done.")


if __name__ == "__main__":
    main()
