# [Parity] collect_set(expr) missing

## Summary

PySpark provides `F.collect_set(expr)` as a built-in aggregate. Robin-sparkless does not expose `collect_set` in the Python API.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
df = create_df([{"k": "a", "v": 1}, {"k": "a", "v": 1}, {"k": "b", "v": 2}], [("k", "string"), ("v", "int")])
df.groupBy("k").agg(rs.collect_set(rs.col("v")).alias("st")).collect()  # AttributeError: no collect_set
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("a", 1), ("a", 1), ("b", 2)], ["k", "v"])
df.groupBy("k").agg(F.collect_set("v").alias("st")).collect()
```

## Expected

Module exposes `collect_set(expr)`. Returns set of unique elements per group.

## Actual

`robin_sparkless` has no `collect_set` symbol.

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/43_collect_set.py`
