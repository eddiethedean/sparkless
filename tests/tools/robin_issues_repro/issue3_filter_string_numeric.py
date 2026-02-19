"""Reproduction for robin-sparkless Issue 3: filter string vs numeric comparison.
PySpark coerces and allows string column == int literal."""
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.createDataFrame([("123",), ("456",)], ["s"])
# String column compared to int: PySpark coerces and evaluates
rows = df.filter(df["s"] == 123).collect()
print("filter(s == 123):", rows)
