# [Parity] any_value(expr [, isIgnoreNull]) missing

## Summary

PySpark provides `F.any_value(expr, isIgnoreNull=False)` as a built-in aggregate. Robin-sparkless does not expose `any_value` in the Python API.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
data = [{"k": "a", "v": 1}, {"k": "a", "v": None}, {"k": "a", "v": 3}]
df = create_df(data, [("k", "string"), ("v", "int")])
out = df.groupBy("k").agg(rs.any_value("v", True).alias("av"))  # AttributeError: no any_value
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("a", 1), ("a", None), ("a", 3)], ["k", "v"])
out = df.groupBy("k").agg(F.any_value("v", True).alias("av"))
out.collect()
```

## Expected

- Module exposes `any_value(expr, isIgnoreNull=False)`.
- When isIgnoreNull is True, returns only non-null values.

## Actual

- `robin_sparkless` has no `any_value` symbol.

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/31_any_value_ignore_nulls.py`
