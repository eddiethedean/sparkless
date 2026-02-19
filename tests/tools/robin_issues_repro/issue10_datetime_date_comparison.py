"""Reproduction for robin-sparkless Issue 10: date vs datetime comparison.
PySpark allows datetime_col < date_literal and coerces for comparison."""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, TimestampType
from datetime import date, datetime

spark = SparkSession.builder.getOrCreate()
# One row: date_col=2024-01-15, ts_col=2024-01-14 23:00:00 (before date)
schema = StructType([
    StructField("ts_col", TimestampType()),
    StructField("date_col", DateType()),
])
df = spark.createDataFrame(
    [(datetime(2024, 1, 14, 23, 0, 0), date(2024, 1, 15))],
    schema,
)
# datetime less than date: 2024-01-14 23:00 < 2024-01-15 -> True
rows = df.filter(df["ts_col"] < df["date_col"]).collect()
print("datetime < date filter:", rows)
print("len:", len(rows))
