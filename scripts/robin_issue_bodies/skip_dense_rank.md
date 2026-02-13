# [Parity] dense_rank() window function missing

## Summary

PySpark provides `F.dense_rank()` as a window function. Robin-sparkless does not expose `dense_rank` (or Window not compatible) in the Python API.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
df = create_df([{"k": "a", "v": 10}, {"k": "a", "v": 20}, {"k": "a", "v": 20}], [("k", "string"), ("v", "int")])
w = rs.Window.partition_by(rs.col("k")).order_by(rs.col("v"))
df.select("k", "v", rs.dense_rank().over(w).alias("rk")).collect()  # no dense_rank or Window
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("a", 10), ("a", 20), ("a", 20)], ["k", "v"])
w = Window.partitionBy("k").orderBy("v")
df.select("k", "v", F.dense_rank().over(w).alias("rk")).collect()
```

## Expected

Module exposes `dense_rank()` usable with Window.

## Actual

`robin_sparkless` has no `dense_rank` (or Window not compatible).

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/55_dense_rank.py`
