"""Reproduction for robin-sparkless Issue 2: array column value must be null or array.
PySpark createDataFrame accepts Python lists for array columns."""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, IntegerType

spark = SparkSession.builder.getOrCreate()
schema = StructType([
    StructField("id", StringType()),
    StructField("arr", ArrayType(IntegerType())),
])

df = spark.createDataFrame([("x", [1, 2, 3]), ("y", [4, 5])], schema)
print("array column:", df.collect())
