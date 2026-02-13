# [Parity] max_by(x, y) / min_by(x, y) missing

## Summary

PySpark provides `F.max_by(x, y)` and `F.min_by(x, y)` as built-in aggregates: value of x associated with the maximum (or minimum) value of y. Robin-sparkless does not expose `max_by` or `min_by` in the Python API.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
data = [{"name": "a", "score": 10}, {"name": "b", "score": 30}, {"name": "c", "score": 20}]
df = create_df(data, [("name", "string"), ("score", "int")])
out = df.agg(rs.max_by(rs.col("name"), rs.col("score")).alias("top"))  # AttributeError: no max_by
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([("a", 10), ("b", 30), ("c", 20)], ["name", "score"])
out = df.agg(F.max_by("name", "score").alias("top"))
out.collect()  # [('b',)]
```

## Expected

- Module exposes `max_by(x, y)` and `min_by(x, y)`.
- max_by returns the value of x where y is maximum; min_by where y is minimum.

## Actual

- `robin_sparkless` has no `max_by` or `min_by` symbol.

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/33_max_by_min_by.py`
