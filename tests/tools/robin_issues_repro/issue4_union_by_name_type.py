"""Reproduction for robin-sparkless Issue 4: unionByName with different column types.
PySpark unionByName coerces/promotes when same-named columns have different types."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df1 = spark.createDataFrame([(1, "a")], ["id", "name"])
df2 = spark.createDataFrame([("2", "b")], ["id", "name"])  # id is string
rows = df1.unionByName(df2).collect()
print("unionByName:", rows)
