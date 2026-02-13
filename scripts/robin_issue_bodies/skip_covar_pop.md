# [Parity] covar_pop(expr1, expr2) missing

## Summary

PySpark provides `F.covar_pop(expr1, expr2)` (population covariance). Robin-sparkless does not expose `covar_pop` in the Python API.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
df = create_df([{"k": "a", "x": 1, "y": 1}, {"k": "a", "x": 2, "y": 2}, {"k": "a", "x": 3, "y": 3}], [("k", "string"), ("x", "int"), ("y", "int")])
df.groupBy("k").agg(rs.covar_pop(rs.col("x"), rs.col("y")).alias("c")).collect()  # AttributeError: no covar_pop
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("a", 1, 1), ("a", 2, 2), ("a", 3, 3)], ["k", "x", "y"])
df.groupBy("k").agg(F.covar_pop("x", "y").alias("c")).collect()
```

## Expected

Module exposes `covar_pop(expr1, expr2)`.

## Actual

`robin_sparkless` has no `covar_pop` symbol.

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/45_covar_pop.py`
