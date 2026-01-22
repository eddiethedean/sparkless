#!/usr/bin/env python3
"""Debug script to see what values are returned from window arithmetic."""

from tests.fixtures.spark_imports import get_spark_imports
from sparkless.session import SparkSession

imports = get_spark_imports()
F = imports.F
Window = imports.Window

# Create a simple spark session

spark = SparkSession.builder.appName("test").getOrCreate()

# Test 1: Simple case
df = spark.createDataFrame(
    [
        {"val": 1},
        {"val": None},
        {"val": 3},
    ]
)
window = Window.orderBy("val")

result = df.select(
    F.col("val"),
    (F.row_number().over(window) * 2).alias("row_times_2"),
)

rows = result.collect()
print("Test 1 - Simple case:")
for i, row in enumerate(rows):
    print(
        f"  Row {i}: val={row['val']}, row_times_2={row['row_times_2']} (type: {type(row['row_times_2'])})"
    )

# Test 2: Complex nested
df2 = spark.createDataFrame(
    [
        {"val": 1},
        {"val": 2},
        {"val": 3},
    ]
)
window2 = Window.orderBy("val")

result2 = df2.select(
    F.col("val"),
    (((F.row_number().over(window2) * 2) + 10) / 3).alias("complex"),
)
rows2 = result2.collect()
print("\nTest 2 - Complex nested:")
for i, row in enumerate(rows2):
    print(
        f"  Row {i}: val={row['val']}, complex={row['complex']} (type: {type(row['complex'])})"
    )

# Test 3: withColumn
df3 = spark.createDataFrame(
    [
        {"dept": "A", "salary": 100},
        {"dept": "A", "salary": 200},
        {"dept": "A", "salary": 300},
    ]
)
window3 = Window.partitionBy("dept").orderBy("salary")

result3 = df3.withColumn("percentile", (F.percent_rank().over(window3) * 100)).select(
    "salary", "percentile"
)
rows3 = result3.collect()
print("\nTest 3 - withColumn:")
for i, row in enumerate(rows3):
    print(
        f"  Row {i}: salary={row['salary']}, percentile={row['percentile']} (type: {type(row['percentile'])})"
    )
