# [Parity] try_sum(expr) / try_avg(expr) missing

## Summary

PySpark provides `F.try_sum(expr)` and `F.try_avg(expr)` as built-in aggregates that return null on overflow instead of throwing. Robin-sparkless does not expose `try_sum` (or `try_avg`) in the Python API.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
data = [{"v": 1}, {"v": 2}, {"v": 3}]
df = create_df(data, [("v", "int")])
out = df.agg(rs.try_sum(rs.col("v")).alias("s"))  # AttributeError: no try_sum
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(1,), (2,), (3,)], ["v"])
out = df.agg(F.try_sum("v").alias("s"))
out.collect()  # [(6,)]
```

## Expected

- Module exposes `try_sum(expr)` and `try_avg(expr)`.
- Same as sum/avg but result is null on overflow.

## Actual

- `robin_sparkless` has no `try_sum` symbol.

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/35_try_sum.py`
