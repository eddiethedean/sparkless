# [Parity] bool_and(expr) / every(expr) missing

## Summary

PySpark provides `F.every(expr)` and `F.bool_and(expr)` (true if all values are true). Robin-sparkless does not expose these in the Python API.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
df = create_df([{"k": "a", "v": True}, {"k": "a", "v": True}], [("k", "string"), ("v", "boolean")])
df.groupBy("k").agg(rs.every(rs.col("v")).alias("ba")).collect()  # AttributeError: no bool_and or every
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("a", True), ("a", True)], ["k", "v"])
df.groupBy("k").agg(F.every("v").alias("ba")).collect()
```

## Expected

Module exposes `every(expr)` or `bool_and(expr)`.

## Actual

`robin_sparkless` has no `every` or `bool_and` symbol.

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/46_bool_and_every.py`
