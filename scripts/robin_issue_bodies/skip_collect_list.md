# [Parity] collect_list(expr) missing

## Summary

PySpark provides `F.collect_list(expr)` as a built-in aggregate. Robin-sparkless does not expose `collect_list` in the Python API.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
df = create_df([{"k": "a", "v": 1}, {"k": "a", "v": 2}, {"k": "b", "v": 3}], [("k", "string"), ("v", "int")])
df.groupBy("k").agg(rs.collect_list(rs.col("v")).alias("lst")).collect()
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("a", 1), ("a", 2), ("b", 3)], ["k", "v"])
df.groupBy("k").agg(F.collect_list("v").alias("lst")).collect()
```

## Expected

Module exposes `collect_list(expr)`. Returns list of non-unique elements per group.

## Actual

`robin_sparkless` has no `collect_list` symbol.

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/42_collect_list.py`
