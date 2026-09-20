import findspark
findspark.init()

import os
from pyspark.sql import SparkSession  # type: ignore[import-untyped]
from pyspark import SparkContext  # type: ignore[import-untyped]
from py4j.java_gateway import JavaGateway, JVMView  # type: ignore[import-untyped]

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_PATH = os.path.normpath(os.path.join(SCRIPT_DIR, "..", "GEO_Download", "data"))
SPARK_MASTER = "spark://localhost:7078"

spark = SparkSession.builder \
    .master(SPARK_MASTER) \
    .appName("pyspark_csv_test") \
    .getOrCreate()

sc: SparkContext = spark.sparkContext
jvm: JVMView = sc._jvm  # type: ignore[assignment]
jsc = sc._jsc  # type: ignore[attr-defined]

from py4j.java_gateway import java_import
java_import(jvm, "org.apache.hadoop.fs.*")

fs = jvm.FileSystem.get(jsc.hadoopConfiguration())
Path = jvm.Path

print(f"Connected to: {sc.master}")
print(f"Spark version: {spark.version}")
print(f"Data path:     {DATA_PATH}")
print()

try:
    statuses = fs.listStatus(Path(DATA_PATH))
    if not statuses:
        print(f"No entries found in {DATA_PATH}")
    else:
        for status in sorted(statuses, key=lambda s: s.getPath().getName()):
            name = status.getPath().getName()
            if status.isDirectory():
                inner = fs.listStatus(Path(f"{DATA_PATH}/{name}"))
                csv_count = sum(1 for s in inner if s.getPath().getName().endswith(".csv"))
                if csv_count > 0:
                    print(f"  {name:30s} -> {csv_count} CSV file(s)")
                else:
                    print(f"  {name:30s} -> {len(inner)} non-CSV file(s)")
            else:
                print(f"  {name:30s} -> (file)")
finally:
    spark.stop()
    print("\nDone.")
