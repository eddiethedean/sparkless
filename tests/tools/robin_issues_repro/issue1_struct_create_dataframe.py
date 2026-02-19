"""Reproduction for robin-sparkless Issue 1: struct value must be object or array.
PySpark accepts both dict and tuple/list for struct columns in createDataFrame."""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

spark = SparkSession.builder.getOrCreate()
schema = StructType([
    StructField("id", StringType()),
    StructField("nested", StructType([
        StructField("a", IntegerType()),
        StructField("b", StringType()),
    ])),
])

# PySpark accepts dict for struct
df1 = spark.createDataFrame([("x", {"a": 1, "b": "y"})], schema)
print("dict struct:", df1.collect())

# PySpark also accepts tuple/list for struct (positional)
df2 = spark.createDataFrame([("x", (1, "y"))], schema)
print("tuple struct:", df2.collect())
