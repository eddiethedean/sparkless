# [Parity] count_if(expr) missing

## Summary

PySpark provides `F.count_if(expr)` as a built-in aggregate that returns the number of TRUE values for the expression. Robin-sparkless does not expose `count_if` in the Python API.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
data = [{"v": 0}, {"v": 1}, {"v": 2}, {"v": 3}, {"v": None}]
df = create_df(data, [("v", "int")])
out = df.agg(rs.count_if(rs.col("v") % 2 == 0).alias("c"))  # AttributeError: no count_if
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(0,), (1,), (2,), (3,), (None,)], ["v"])
out = df.agg(F.count_if(F.col("v") % 2 == 0).alias("c"))
out.collect()  # [(2,)]
```

## Expected

- Module exposes `count_if(expr)`.
- Returns the number of rows where expr is TRUE.

## Actual

- `robin_sparkless` has no `count_if` symbol.

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/32_count_if.py`
