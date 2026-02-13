# [Parity] element_at(col, index) missing

## Summary

PySpark provides `F.element_at(col, index)` for 1-based indexing into array or map. Robin-sparkless does not expose `element_at` in the Python API.

## To reproduce (Robin)

```python
import robin_sparkless as rs
spark = rs.SparkSession.builder().app_name("repro").get_or_create()
create_df = spark.create_dataframe_from_rows
df = create_df([{"arr": [10, 20, 30]}], [("arr", "array")])
df.select(rs.element_at(rs.col("arr"), 2).alias("v")).collect()  # AttributeError: no element_at
```

## PySpark equivalent

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[1]").getOrCreate()
df = spark.createDataFrame([([10, 20, 30],)], "arr array<int>")
df.select(F.element_at("arr", 2).alias("v")).collect()  # 20
```

## Expected

Module exposes `element_at(col, index)` (1-based).

## Actual

`robin_sparkless` has no `element_at` symbol.

## How to reproduce

Run from Sparkless repo root: `python scripts/robin_parity_repros/53_element_at.py`
