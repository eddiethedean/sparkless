# [Parity] approx_percentile(col, percentage [, accuracy]) missing

## Summary

PySpark provides `F.approx_percentile(col, percentage, accuracy)` as a built-in aggregate (Spark SQL approx_percentile). Robin-sparkless does not expose `approx_percentile` in the Python API.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
df = create_df([{"v": i} for i in (0, 1, 2, 10)], [("v", "int")])
out = df.agg(rs.approx_percentile(rs.col("v"), 0.5).alias("p"))  # AttributeError: no approx_percentile
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([(0,), (1,), (2,), (10,)], ["v"])
out = df.agg(F.approx_percentile("v", 0.5).alias("p"))
out.collect()
```

## Expected

- Module exposes `approx_percentile(col, percentage, accuracy=None)`.
- Returns approximate percentile of the numeric column.

## Actual

- `robin_sparkless` has no `approx_percentile` symbol.

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/30_approx_percentile.py`
