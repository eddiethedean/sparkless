# [Parity] corr(expr1, expr2) missing

## Summary

PySpark provides `F.corr(expr1, expr2)` (Pearson correlation). Robin-sparkless does not expose `corr` in the Python API.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
df = create_df([{"k": "a", "x": 1, "y": 2}, {"k": "a", "x": 2, "y": 4}, {"k": "a", "x": 3, "y": 6}], [("k", "string"), ("x", "int"), ("y", "int")])
df.groupBy("k").agg(rs.corr(rs.col("x"), rs.col("y")).alias("r")).collect()  # AttributeError: no corr
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("a", 1, 2), ("a", 2, 4), ("a", 3, 6)], ["k", "x", "y"])
df.groupBy("k").agg(F.corr("x", "y").alias("r")).collect()
```

## Expected

Module exposes `corr(expr1, expr2)`.

## Actual

`robin_sparkless` has no `corr` symbol.

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/44_corr.py`
