"""Reproduction for robin-sparkless Issue 5: join column resolution (e.g. not found: ID).
PySpark resolves id/ID in join result."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df1 = spark.createDataFrame([(1, "x")], ["id", "val"])
df2 = spark.createDataFrame([(1, "y")], ["ID", "other"])  # different case
j = df1.join(df2, df1["id"] == df2["ID"])
rows = j.collect()
print("join result:", rows)
print("columns:", j.columns)
