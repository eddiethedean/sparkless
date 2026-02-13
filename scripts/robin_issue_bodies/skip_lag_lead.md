# [Parity] lag(col, offset) / lead(col, offset) missing

## Summary

PySpark provides `F.lag(col, offset)` and `F.lead(col, offset)` as window functions. Robin-sparkless does not expose `lag` (or script reports no lag or Window) in the Python API for use with Window.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
df = create_df([{"k": "a", "v": 1}, {"k": "a", "v": 2}, {"k": "a", "v": 3}], [("k", "string"), ("v", "int")])
w = rs.Window.partition_by(rs.col("k")).order_by(rs.col("v"))
df.select("k", "v", rs.lag(rs.col("v"), 1).over(w).alias("prev")).collect()  # no lag or Window
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("a", 1), ("a", 2), ("a", 3)], ["k", "v"])
w = Window.partitionBy("k").orderBy("v")
df.select("k", "v", F.lag("v", 1).over(w).alias("prev")).collect()
```

## Expected

Module exposes `lag(col, offset)` and `lead(col, offset)` usable with Window.

## Actual

`robin_sparkless` has no `lag` (or Window not compatible).

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/54_lag_lead.py`
